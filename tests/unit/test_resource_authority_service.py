from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from rquant.external_monotonic_root import UnixSocketExternalMonotonicRootManifest
from rquant.external_monotonic_root_service import (
    ExternalRootServiceConfiguration,
)
from rquant.lab_resource_authority_adapter import (
    ExternalResourceJournalRootConfig,
    LabResourceAuthorityReservationAdapter,
    ResourceAuthorityAdapterConfig,
    ResourceAuthorityAdapterTransportError,
)
from rquant.resource_authority_service import (
    ExternalMonotonicRootDaemonConfiguration,
    ResourceAuthorityDaemonConfiguration,
    ResourceAuthorityServiceConfiguration,
    ResourceAuthorityServiceError,
    ResourceAuthorityUnixService,
    TrustedRoleInventoryConfiguration,
    compose_external_monotonic_root_daemon,
    compose_resource_authority_daemon,
    compose_resource_authority_service,
    load_external_monotonic_root_daemon_configuration,
    load_resource_authority_daemon_configuration,
    probe_resource_authority_service,
    verify_authority_os_isolation,
)
from rquant.resource_journal_high_water import (
    RESOURCE_JOURNAL_HEAD_NAMESPACE,
    RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    TRUSTED_RESOURCE_ROLE_PURPOSES,
    TrustedRoleInventory,
)
from rquant.runtime_resource_admission import (
    RESOURCE_OPERATION_KEY_PURPOSE,
    RESOURCE_OPERATION_RECEIPT_NAMESPACE,
)
from rquant.strict_json import canonical_json_bytes

from .test_external_monotonic_root_service import _private_socket_parent, _signing_pair
from .test_lab_resource_authority_adapter import _identity, _policy, _request, _snapshot

_OPERATION_NAMESPACES = frozenset(
    {
        RESOURCE_OPERATION_RECEIPT_NAMESPACE,
        "rquant-resource-admission-genesis/v1",
        RESOURCE_JOURNAL_HEAD_NAMESPACE,
    }
)


def _inventory(operation_fingerprint: str, root_fingerprint: str) -> TrustedRoleInventory:
    fingerprints = {
        purpose: frozenset({hashlib.sha256(f"test:{purpose}".encode()).hexdigest()})
        for purpose in TRUSTED_RESOURCE_ROLE_PURPOSES
    }
    fingerprints[RESOURCE_OPERATION_KEY_PURPOSE] = frozenset({operation_fingerprint})
    fingerprints[RESOURCE_JOURNAL_HIGH_WATER_PURPOSE] = frozenset({root_fingerprint})
    return TrustedRoleInventory(role_fingerprints=fingerprints)


def _start(service: object) -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()
    thread = threading.Thread(
        target=service.serve_forever,
        kwargs={"stop": stop},
        daemon=True,
    )
    thread.start()
    assert service.ready.wait(timeout=5)
    return stop, thread


def _stop(service: object, stop: threading.Event, thread: threading.Thread) -> None:
    stop.set()
    service.wake()
    thread.join(timeout=5)
    assert not thread.is_alive()


def test_two_daemon_roundtrip_response_loss_and_resource_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket_root = Path(tempfile.mkdtemp(prefix="rs-", dir=_private_socket_parent())).resolve()
    os.chown(socket_root, os.getuid(), os.getgid())
    socket_root.chmod(0o700)
    root_socket_root = socket_root / "e"
    resource_socket_root = socket_root / "r"
    for directory in (root_socket_root, resource_socket_root):
        directory.mkdir()
        directory.chmod(0o750)
    root_signer, root_verifier = _signing_pair(tmp_path / "root-keys")
    operation_signer, operation_verifier = _signing_pair(
        tmp_path / "operation-keys",
        issuer="resource-operation-issuer",
        key_id="resource-operation-key",
        key_purpose=RESOURCE_OPERATION_KEY_PURPOSE,
        namespaces=_OPERATION_NAMESPACES,
    )
    root_socket = root_socket_root / "root.sock"
    resource_socket = resource_socket_root / "resource.sock"
    root_manifest = UnixSocketExternalMonotonicRootManifest(
        role="resource_journal_monotonic_root",
        authority_id="resource-external-root",
        store_id="resource-external-root-store",
        rollback_domain_id="external-resource-root-domain",
        socket_path=root_socket,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        socket_mode=0o660,
        peer_uid=os.getuid(),
        peer_gid=os.getgid(),
        connect_timeout_ms=2_000,
        max_response_bytes=1024 * 1024,
    )
    root_service_config = ExternalRootServiceConfiguration(
        socket_path=root_socket,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        service_uid=os.getuid(),
        service_gid=os.getgid(),
        allowed_peer_uid=os.getuid(),
        allowed_peer_gid=os.getgid(),
        socket_mode=0o660,
        socket_directory_mode=0o750,
        role=root_manifest.role,
        authority_id=root_manifest.authority_id,
        store_id=root_manifest.store_id,
        rollback_domain_id=root_manifest.rollback_domain_id,
        transport_manifest_hash=root_manifest.manifest_hash,
    )
    root_daemon_config = ExternalMonotonicRootDaemonConfiguration(
        service_configuration=root_service_config,
        backend_path=tmp_path / "external-root.sqlite3",
        high_water_authority_id="resource-high-water",
        private_key_path=tmp_path / "root-keys" / "root.private.pem",
        public_key_path=tmp_path / "root-keys" / "root.public.pem",
        issuer=root_signer.issuer,
        key_id=root_signer.key_id,
    )
    root_config_path = tmp_path / "external-root-config.json"
    root_config_path.write_bytes(canonical_json_bytes(root_daemon_config.model_dump(mode="json")))
    root_config_path.chmod(0o600)
    root_service = compose_external_monotonic_root_daemon(
        load_external_monotonic_root_daemon_configuration(root_config_path)
    )
    root_stop, root_thread = _start(root_service)

    external_config = ExternalResourceJournalRootConfig(
        transport="unix-socket-v1",
        transport_manifest_hash=root_manifest.manifest_hash,
        root_authority_id=root_manifest.authority_id,
        root_store_id=root_manifest.store_id,
        root_issuer=root_verifier.issuer,
        root_key_id=root_verifier.key_id,
        root_public_key_fingerprint=root_verifier.public_key_fingerprint,
        witness_rollback_domain_id=root_manifest.rollback_domain_id,
        local_rollback_domain_id="resource-authority-domain",
    )
    inventory = _inventory(
        operation_verifier.public_key_fingerprint,
        root_verifier.public_key_fingerprint,
    )
    adapter_config = ResourceAuthorityAdapterConfig(
        mode="production",
        endpoint=resource_socket,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_server_uid=os.getuid(),
        expected_server_gid=os.getgid(),
        allowed_peer_uid=os.getuid(),
        allowed_peer_gid=os.getgid(),
        socket_mode=0o660,
        socket_directory_mode=0o750,
        authority_id="resource-authority",
        high_water_authority_id="resource-high-water",
        external_root_config=external_config,
        trusted_role_inventory_hash=inventory.policy_hash,
        timeout_milliseconds=10_000,
    )
    service_config = ResourceAuthorityServiceConfiguration(
        adapter_configuration=adapter_config,
        external_root_manifest=root_manifest,
        resource_journal_path=tmp_path / "resource-journal.sqlite3",
        high_water_cache_path=tmp_path / "resource-high-water.sqlite3",
        trusted_role_inventory=TrustedRoleInventoryConfiguration(
            roles={
                purpose: tuple(sorted(fingerprints))
                for purpose, fingerprints in inventory.as_json_value().items()
            }
        ),
        trusted_journal_issuer=operation_signer.issuer,
    )
    resource_daemon_config = ResourceAuthorityDaemonConfiguration(
        service_configuration=service_config,
        operation_private_key_path=tmp_path / "operation-keys" / "root.private.pem",
        operation_public_key_path=tmp_path / "operation-keys" / "root.public.pem",
        operation_issuer=operation_signer.issuer,
        operation_key_id=operation_signer.key_id,
        root_public_key_path=tmp_path / "root-keys" / "root.public.pem",
    )
    resource_config_path = tmp_path / "resource-config.json"
    resource_config_path.write_bytes(
        canonical_json_bytes(resource_daemon_config.model_dump(mode="json"))
    )
    resource_config_path.chmod(0o600)

    class _StructuralVerifier:
        issuer = root_verifier.issuer
        key_id = root_verifier.key_id
        key_purpose = root_verifier.key_purpose
        signature_algorithm = root_verifier.signature_algorithm
        public_key_fingerprint = root_verifier.public_key_fingerprint

        def verify(self, *, namespace: str, payload: bytes, signature: str) -> bool:
            return root_verifier.verify(
                namespace=namespace,
                payload=payload,
                signature=signature,
            )

    with pytest.raises(ResourceAuthorityServiceError, match="closed"):
        compose_resource_authority_service(
            configuration=service_config,
            operation_signer=operation_signer,
            operation_verifier=operation_verifier,
            root_verifier=_StructuralVerifier(),  # type: ignore[arg-type]
            policy_provider=_policy,
            snapshot_provider=_snapshot,
        )

    def compose() -> ResourceAuthorityUnixService:
        return compose_resource_authority_daemon(
            configuration=load_resource_authority_daemon_configuration(resource_config_path),
            policy_provider=_policy,
            snapshot_provider=_snapshot,
            clock=lambda: _snapshot().observed_at,
        )

    resource_service = compose()
    resource_stop, resource_thread = _start(resource_service)
    identity = _identity()
    request = _request(identity)
    try:
        probe = probe_resource_authority_service(adapter_config)
        assert probe.identity.authority_id == "resource-authority"
        assert probe.capabilities == ("policy", "snapshot", "journal")
        from rquant.preflight import verify_resource_authority_services

        monkeypatch.setattr(
            "rquant.resource_authority_service.verify_authority_os_isolation",
            lambda *_args: (
                "external-root uid/gid and private state isolated",
                "resource-authority uid/gid and private state isolated",
                "lighthouse restricted to resource socket client group",
            ),
        )

        preflight = verify_resource_authority_services(
            root_config_path,
            resource_config_path,
        )
        assert preflight.status == "ok"
        adapter = LabResourceAuthorityReservationAdapter(adapter_config)
        admitted = adapter.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None
        resource_service.server.drop_next_response_after_effect_for_test("recheck")
        with pytest.raises(ResourceAuthorityAdapterTransportError):
            adapter.recheck(
                lease=admitted.lease,
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
            )
        _stop(resource_service, resource_stop, resource_thread)

        restarted = compose()
        restarted_stop, restarted_thread = _start(restarted)
        try:
            recovered = LabResourceAuthorityReservationAdapter(adapter_config)
            assert recovered.release(admitted.lease, identity=identity) is True
        finally:
            _stop(restarted, restarted_stop, restarted_thread)
    finally:
        if resource_thread.is_alive():
            _stop(resource_service, resource_stop, resource_thread)
        _stop(root_service, root_stop, root_thread)
        root_socket_root.rmdir()
        resource_socket_root.rmdir()
        socket_root.rmdir()


def test_resource_authority_systemd_drafts_are_hardened_and_ordered() -> None:
    root = Path(__file__).resolve().parents[2] / "deploy" / "systemd"
    external = (root / "rquant-external-monotonic-root.service").read_text()
    resource = (root / "rquant-resource-authority.service").read_text()

    for unit in (external, resource):
        assert "NoNewPrivileges=true" in unit
        assert "ProtectSystem=strict" in unit
        assert "RestrictAddressFamilies=AF_UNIX" in unit
        assert "UMask=0077" in unit
        assert "OnCalendar=" not in unit
    assert "Before=rquant-resource-authority.service" in external
    assert "Requires=rquant-external-monotonic-root.service" in resource
    assert "After=rquant-external-monotonic-root.service" in resource
    assert "User=rquant-external-root" in external
    assert "Group=rquant-root-client" in external
    assert "User=rquant-resource-authority" in resource
    assert "Group=rquant-resource-client" in resource
    assert "SupplementaryGroups=rquant-root-client" in resource
    assert "RuntimeDirectory=rquant-external-root" in external
    assert "StateDirectory=rquant-external-root" in external
    assert "RuntimeDirectory=rquant-resource-authority" in resource
    assert "StateDirectory=rquant-resource-authority" in resource
    assert "EnvironmentFile=/etc/rquant/external-root.env" in external
    assert "EnvironmentFile=/etc/rquant/resource-authority.env" in resource
    assert "/home/lighthouse/rquant/.env" not in external
    assert "/home/lighthouse/rquant/.env" not in resource
    assert "/var/lib/rquant-external-root" in external
    assert "/var/lib/rquant-external-root" not in resource
    assert "/etc/rquant/keys/external-root/root.private.pem" in external
    assert "/etc/rquant/keys/external-root/root.private.pem" not in resource
    assert "/var/lib/rquant-resource-authority" in resource
    for unit in (external, resource):
        assert "/home/lighthouse/rquant" not in unit
        assert "/usr/local/libexec/rquant-authority-runtime/current/venv/bin/rquant" in unit
        assert "WorkingDirectory=/usr/local/libexec/rquant-authority-runtime/current" in unit


def test_resource_authority_install_draft_is_explicit_and_does_not_expand_sudo() -> None:
    project_root = Path(__file__).resolve().parents[2]
    script = (project_root / "scripts" / "install-resource-authority-infra.sh").read_text()
    publisher = (project_root / "scripts" / "publish-authority-runtime.py").read_text()
    root_env = (project_root / "deploy" / "env" / "external-root.env.example").read_text()
    resource_env = (project_root / "deploy" / "env" / "resource-authority.env.example").read_text()

    assert 'MODE="dry-run"' in script
    assert '"--apply"' in script
    assert "rquant-external-root" in script
    assert "rquant-resource-authority" in script
    assert "rquant-root-client" in script
    assert "rquant-resource-client" in script
    assert "useradd" in script
    assert "groupadd" in script
    assert "systemctl" not in script
    assert "sudoers" not in script
    assert "NOPASSWD" not in script
    assert "--release-sha" in script
    assert "archive --format=tar" in script
    assert "rquant-authority-runtime/generations" in script
    assert "manifest.json" in publisher
    assert "manifest.sig" in publisher
    assert "pkeyutl" in publisher
    assert 'getattr(os, "O_NOFOLLOW", 0)' in publisher
    assert "dir_fd=" in publisher
    assert "runuser --user" in script
    assert "APP_ENV=prod" in root_env
    assert "RQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH=" in root_env
    assert "APP_ENV=prod" in resource_env
    assert "RQUANT_RESOURCE_AUTHORITY_SERVICE_CONFIG_PATH=" in resource_env
    assert "RQUANT_LAB_RESOURCE_AUTHORITY_CONFIG_JSON=" in resource_env
    assert "DATA_DIR=" not in resource_env
    assert "DUCKDB_PATH=" not in resource_env
    assert "PARQUET_DIR=" not in resource_env
    for forbidden in ("TUSHARE", "PUSHDEER", "PUSHPLUS", "PASSWORD", "COOKIE"):
        assert forbidden not in root_env
        assert forbidden not in resource_env


@pytest.mark.parametrize(
    "payload",
    (
        "APP_ENV=prod\nUNKNOWN=value\n",
        "APP_ENV=prod\nAPP_ENV=prod\n",
        "export APP_ENV=prod\n",
        "APP_ENV =prod\n",
        "APP_ENV=${RQUANT_ENV}\n",
        "APP_ENV=$(id)\n",
        "APP_ENV=prod trailing\n",
    ),
)
def test_authority_environment_parser_rejects_nonclosed_input(
    tmp_path: Path,
    payload: str,
) -> None:
    from rquant.resource_authority_service import (
        EXTERNAL_ROOT_ENVIRONMENT_KEYS,
        load_closed_authority_environment,
    )

    path = tmp_path / "external-root.env"
    path.write_text(payload, encoding="utf-8")
    path.chmod(0o440)

    with pytest.raises(ResourceAuthorityServiceError, match="environment"):
        load_closed_authority_environment(
            path,
            allowed_keys=EXTERNAL_ROOT_ENVIRONMENT_KEYS,
            required_keys=EXTERNAL_ROOT_ENVIRONMENT_KEYS,
            trusted_root=tmp_path,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        )


def test_authority_environment_parser_accepts_only_exact_canonical_keys(tmp_path: Path) -> None:
    from rquant.resource_authority_service import (
        EXTERNAL_ROOT_ENVIRONMENT_KEYS,
        load_closed_authority_environment,
    )

    path = tmp_path / "external-root.env"
    path.write_text(
        "APP_ENV=prod\n"
        "RQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH=/etc/rquant/external-monotonic-root.json\n",
        encoding="utf-8",
    )
    path.chmod(0o440)

    values = load_closed_authority_environment(
        path,
        allowed_keys=EXTERNAL_ROOT_ENVIRONMENT_KEYS,
        required_keys=EXTERNAL_ROOT_ENVIRONMENT_KEYS,
        trusted_root=tmp_path,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
    )

    assert values == {
        "APP_ENV": "prod",
        "RQUANT_EXTERNAL_MONOTONIC_ROOT_SERVICE_CONFIG_PATH": (
            "/etc/rquant/external-monotonic-root.json"
        ),
    }


def test_daemon_config_loader_rejects_writable_or_symlinked_ancestor(tmp_path: Path) -> None:
    from rquant.resource_authority_service import load_closed_authority_environment

    safe = tmp_path / "safe"
    safe.mkdir(mode=0o755)
    env = safe / "authority.env"
    env.write_text("APP_ENV=prod\n", encoding="utf-8")
    env.chmod(0o440)

    safe.chmod(0o775)
    with pytest.raises(ResourceAuthorityServiceError, match="ancestor"):
        load_closed_authority_environment(
            env,
            allowed_keys=frozenset({"APP_ENV"}),
            required_keys=frozenset({"APP_ENV"}),
            trusted_root=tmp_path,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        )

    safe.chmod(0o755)
    renamed = tmp_path / "safe-real"
    safe.rename(renamed)
    safe.symlink_to(renamed.name, target_is_directory=True)
    with pytest.raises(ResourceAuthorityServiceError, match="ancestor"):
        load_closed_authority_environment(
            safe / "authority.env",
            allowed_keys=frozenset({"APP_ENV"}),
            required_keys=frozenset({"APP_ENV"}),
            trusted_root=tmp_path,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        )


def test_authority_runtime_release_verifies_signature_hashes_and_ancestors(
    tmp_path: Path,
) -> None:
    from rquant.authority_runtime_release import (
        AuthorityRuntimeFile,
        AuthorityRuntimeManifest,
        AuthorityRuntimeReleaseError,
        verify_authority_runtime_release,
    )

    runtime = tmp_path / "usr/local/libexec/rquant-authority-runtime"
    payload = runtime / "generations/staging/payload"
    executable = payload / "venv/bin/rquant"
    executable.parent.mkdir(parents=True, mode=0o755)
    executable.write_bytes(b"#!/bin/sh\nexit 0\n")
    executable.chmod(0o555)
    for directory in (payload, payload / "venv", payload / "venv/bin"):
        directory.chmod(0o555)
    manifest = AuthorityRuntimeManifest(
        release_sha="1" * 40,
        publisher_sha256="2" * 64,
        publisher_version="rquant-authority-runtime-publisher/v2",
        executable="venv/bin/rquant",
        files=(
            AuthorityRuntimeFile(
                path="venv/bin/rquant",
                sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
                size=executable.stat().st_size,
                mode=0o555,
            ),
        ),
    )
    manifest_bytes = canonical_json_bytes(manifest.model_dump(mode="json"))
    generation_id = manifest.release_sha
    generation = runtime / "generations" / generation_id
    (runtime / "generations/staging").rename(generation)
    executable = generation / "payload/venv/bin/rquant"
    manifest_path = generation / "manifest.json"
    manifest_path.write_bytes(manifest_bytes)
    manifest_path.chmod(0o444)
    manifest_hash_path = generation / "manifest.sha256"
    manifest_hash_path.write_text(
        f"{hashlib.sha256(manifest_bytes).hexdigest()}\n",
        encoding="ascii",
    )
    manifest_hash_path.chmod(0o444)

    private_key = tmp_path / "runtime.private.pem"
    public_key = tmp_path / "runtime.public.pem"
    subprocess.run(
        ("openssl", "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        (
            "openssl",
            "pkey",
            "-in",
            str(private_key),
            "-pubout",
            "-out",
            str(public_key),
        ),
        check=True,
        capture_output=True,
    )
    public_key.chmod(0o444)
    signature = generation / "manifest.sig"
    subprocess.run(
        (
            "openssl",
            "pkeyutl",
            "-sign",
            "-rawin",
            "-inkey",
            str(private_key),
            "-in",
            str(manifest_path),
            "-out",
            str(signature),
        ),
        check=True,
        capture_output=True,
    )
    signature.chmod(0o444)
    generation.chmod(0o555)
    (runtime / "generations").chmod(0o755)
    runtime.chmod(0o755)
    (runtime / "current").symlink_to(f"generations/{generation_id}")

    verified = verify_authority_runtime_release(
        root=runtime,
        signing_public_key_path=public_key,
        trusted_root=tmp_path,
        expected_uid=os.geteuid(),
        expected_gid=os.getegid(),
        signing_key_uid=os.geteuid(),
        signing_key_gid=os.getegid(),
        expected_publisher_sha256="2" * 64,
        expected_publisher_version="rquant-authority-runtime-publisher/v2",
    )
    assert verified.release_sha == "1" * 40
    assert verified.generation_id == generation_id

    runtime_ancestor = runtime.parent
    runtime_ancestor.chmod(0o775)
    with pytest.raises(AuthorityRuntimeReleaseError, match="runtime"):
        verify_authority_runtime_release(
            root=runtime,
            signing_public_key_path=public_key,
            trusted_root=tmp_path,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
            signing_key_uid=os.geteuid(),
            signing_key_gid=os.getegid(),
            expected_publisher_sha256="2" * 64,
            expected_publisher_version="rquant-authority-runtime-publisher/v2",
        )
    runtime_ancestor.chmod(0o755)

    executable.chmod(0o755)
    with pytest.raises(AuthorityRuntimeReleaseError, match="runtime"):
        verify_authority_runtime_release(
            root=runtime,
            signing_public_key_path=public_key,
            trusted_root=tmp_path,
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
            signing_key_uid=os.geteuid(),
            signing_key_gid=os.getegid(),
            expected_publisher_sha256="2" * 64,
            expected_publisher_version="rquant-authority-runtime-publisher/v2",
        )


def test_external_root_daemon_config_loader_rejects_wide_mode_and_symlink(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "root.sock"
    manifest = UnixSocketExternalMonotonicRootManifest(
        role="resource_journal_monotonic_root",
        authority_id="root-authority",
        store_id="root-store",
        rollback_domain_id="root-domain",
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        socket_mode=0o660,
        peer_uid=os.getuid(),
        peer_gid=os.getgid(),
        connect_timeout_ms=1_000,
        max_response_bytes=1_024,
    )
    configuration = ExternalMonotonicRootDaemonConfiguration(
        service_configuration=ExternalRootServiceConfiguration(
            socket_path=socket_path,
            socket_uid=os.getuid(),
            socket_gid=os.getgid(),
            service_uid=os.getuid(),
            service_gid=os.getgid(),
            allowed_peer_uid=os.getuid(),
            allowed_peer_gid=os.getgid(),
            socket_mode=0o660,
            socket_directory_mode=0o750,
            role=manifest.role,
            authority_id=manifest.authority_id,
            store_id=manifest.store_id,
            rollback_domain_id=manifest.rollback_domain_id,
            transport_manifest_hash=manifest.manifest_hash,
        ),
        backend_path=tmp_path / "root.sqlite3",
        high_water_authority_id="high-water",
        private_key_path=tmp_path / "root.private.pem",
        public_key_path=tmp_path / "root.public.pem",
        issuer="root-issuer",
        key_id="root-key",
    )
    path = tmp_path / "root-config.json"
    path.write_bytes(canonical_json_bytes(configuration.model_dump(mode="json")))
    path.chmod(0o644)
    with pytest.raises(ResourceAuthorityServiceError, match="unsafe"):
        load_external_monotonic_root_daemon_configuration(path)

    path.chmod(0o600)
    assert load_external_monotonic_root_daemon_configuration(path) == configuration
    path.chmod(0o444)
    assert load_external_monotonic_root_daemon_configuration(path) == configuration
    alias = tmp_path / "root-config-link.json"
    alias.symlink_to(path)
    with pytest.raises(ResourceAuthorityServiceError, match="unsafe"):
        load_external_monotonic_root_daemon_configuration(alias)


def test_authority_os_isolation_verifies_static_principals_and_denies_cross_access(
    tmp_path: Path,
) -> None:
    root_uid, root_gid = 2101, 2201
    resource_uid, resource_gid = 2102, 2202
    app_uid, app_gid = os.getuid(), os.getgid()
    root_runtime = tmp_path / "run" / "external-root"
    resource_runtime = tmp_path / "run" / "resource-authority"
    root_state = tmp_path / "state" / "external-root"
    resource_state = tmp_path / "state" / "resource-authority"
    root_key_root = tmp_path / "keys" / "external-root"
    resource_key_root = tmp_path / "keys" / "resource-authority"
    root_private = root_key_root / "root.private.pem"
    root_public = root_key_root / "root.public.pem"
    operation_private = resource_key_root / "operation.private.pem"
    operation_public = resource_key_root / "operation.public.pem"
    paths = {
        root_runtime: (stat.S_IFDIR | 0o750, root_uid, root_gid),
        resource_runtime: (stat.S_IFDIR | 0o750, resource_uid, resource_gid),
        root_state: (stat.S_IFDIR | 0o700, root_uid, root_gid),
        resource_state: (stat.S_IFDIR | 0o700, resource_uid, resource_gid),
        root_key_root: (stat.S_IFDIR | 0o750, root_uid, root_gid),
        resource_key_root: (stat.S_IFDIR | 0o750, resource_uid, resource_gid),
        root_runtime / "root.sock": (stat.S_IFSOCK | 0o660, root_uid, root_gid),
        resource_runtime / "resource.sock": (
            stat.S_IFSOCK | 0o660,
            resource_uid,
            resource_gid,
        ),
        root_state / "root.sqlite3": (stat.S_IFREG | 0o600, root_uid, root_gid),
        resource_state / "journal.sqlite3": (
            stat.S_IFREG | 0o600,
            resource_uid,
            resource_gid,
        ),
        resource_state / "high-water.sqlite3": (
            stat.S_IFREG | 0o600,
            resource_uid,
            resource_gid,
        ),
        root_private: (stat.S_IFREG | 0o400, root_uid, root_gid),
        root_public: (stat.S_IFREG | 0o440, 0, root_gid),
        operation_private: (stat.S_IFREG | 0o400, resource_uid, resource_gid),
        operation_public: (stat.S_IFREG | 0o440, resource_uid, resource_gid),
    }
    for path in paths:
        if stat.S_ISDIR(paths[path][0]):
            path.mkdir(parents=True, exist_ok=True)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

    users = {
        "rquant-external-root": SimpleNamespace(pw_uid=root_uid, pw_gid=root_gid),
        "rquant-resource-authority": SimpleNamespace(
            pw_uid=resource_uid,
            pw_gid=resource_gid,
        ),
        "lighthouse": SimpleNamespace(pw_uid=app_uid, pw_gid=app_gid),
    }
    groups = {
        "rquant-root-client": SimpleNamespace(
            gr_gid=root_gid,
            gr_mem=["rquant-resource-authority"],
        ),
        "rquant-resource-client": SimpleNamespace(
            gr_gid=resource_gid,
            gr_mem=["lighthouse"],
        ),
    }

    def metadata(path: Path) -> object:
        mode, uid, gid = paths[path]
        return SimpleNamespace(st_mode=mode, st_uid=uid, st_gid=gid)

    def access(
        path: Path,
        requested: int,
        uid: int,
        primary_gid: int,
        supplementary_gids: frozenset[int],
    ) -> bool:
        mode, owner_uid, owner_gid = paths[path]
        if uid == owner_uid:
            bits = (mode >> 6) & 0o7
        elif owner_gid in {primary_gid, *supplementary_gids}:
            bits = (mode >> 3) & 0o7
        else:
            bits = mode & 0o7
        required = (
            (0o4 if requested & os.R_OK else 0)
            | (0o2 if requested & os.W_OK else 0)
            | (0o1 if requested & os.X_OK else 0)
        )
        return bits & required == required

    root_daemon = SimpleNamespace(
        service_configuration=SimpleNamespace(
            socket_path=root_runtime / "root.sock",
            socket_uid=root_uid,
            socket_gid=root_gid,
            service_uid=root_uid,
            service_gid=root_gid,
            allowed_peer_uid=resource_uid,
            allowed_peer_gid=resource_gid,
            socket_mode=0o660,
            socket_directory_mode=0o750,
        ),
        backend_path=root_state / "root.sqlite3",
        private_key_path=root_private,
        public_key_path=root_public,
    )
    resource_daemon = SimpleNamespace(
        service_configuration=SimpleNamespace(
            adapter_configuration=SimpleNamespace(
                endpoint=resource_runtime / "resource.sock",
                expected_uid=resource_uid,
                expected_gid=resource_gid,
                expected_server_uid=resource_uid,
                expected_server_gid=resource_gid,
                allowed_peer_uid=app_uid,
                allowed_peer_gid=app_gid,
                socket_mode=0o660,
                socket_directory_mode=0o750,
            ),
            resource_journal_path=resource_state / "journal.sqlite3",
            high_water_cache_path=resource_state / "high-water.sqlite3",
        ),
        operation_private_key_path=operation_private,
        operation_public_key_path=operation_public,
        root_public_key_path=root_public,
    )

    details = verify_authority_os_isolation(
        root_daemon,
        resource_daemon,
        user_lookup=users.__getitem__,
        group_lookup=groups.__getitem__,
        metadata_lookup=metadata,
        access_check=access,
    )
    assert details == (
        "external-root uid/gid and private state isolated",
        "resource-authority uid/gid and private state isolated",
        "lighthouse restricted to resource socket client group",
    )

    groups["rquant-root-client"].gr_mem = []
    with pytest.raises(ResourceAuthorityServiceError, match="root client group"):
        verify_authority_os_isolation(
            root_daemon,
            resource_daemon,
            user_lookup=users.__getitem__,
            group_lookup=groups.__getitem__,
            metadata_lookup=metadata,
            access_check=access,
        )

    groups["rquant-root-client"].gr_mem = ["rquant-resource-authority"]

    def leaked_access(
        path: Path,
        requested: int,
        uid: int,
        primary_gid: int,
        supplementary_gids: frozenset[int],
    ) -> bool:
        if path == root_private and uid == resource_uid and requested & os.R_OK:
            return True
        return access(path, requested, uid, primary_gid, supplementary_gids)

    with pytest.raises(ResourceAuthorityServiceError, match="read root private key"):
        verify_authority_os_isolation(
            root_daemon,
            resource_daemon,
            user_lookup=users.__getitem__,
            group_lookup=groups.__getitem__,
            metadata_lookup=metadata,
            access_check=leaked_access,
        )

    with pytest.raises(ResourceAuthorityServiceError, match="system user or group"):
        verify_authority_os_isolation(
            root_daemon,
            resource_daemon,
            user_lookup=lambda _name: (_ for _ in ()).throw(KeyError("missing")),
            group_lookup=groups.__getitem__,
            metadata_lookup=metadata,
            access_check=access,
        )
