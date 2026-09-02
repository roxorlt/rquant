"""Closed-resource-authority adapter contracts for spawned Lab workers."""

from __future__ import annotations

import errno
import hashlib
import hmac
import multiprocessing
import os
import select
import socket
import sqlite3
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from pydantic import ValidationError

from rquant import lab_resource_authority_adapter as adapter_module
from rquant.lab_resource_authority_adapter import (
    LAB_RESOURCE_AUTHORITY_REGISTRY_HASH,
    LAB_RESOURCE_AUTHORITY_REGISTRY_ID,
    LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION,
    RESOURCE_AUTHORITY_ADAPTER_MAX_WIRE_BYTES,
    RESOURCE_AUTHORITY_MAX_LOCK_WAIT_MILLISECONDS,
    ExternalResourceJournalMonotonicRootAdapter,
    ExternalResourceJournalRootConfig,
    LabResourceAuthorityReservationAdapter,
    ResourceAuthorityAdapterConfig,
    ResourceAuthorityAdapterConfigurationError,
    ResourceAuthorityAdapterIdentity,
    ResourceAuthorityAdapterRemoteError,
    ResourceAuthorityAdapterRequest,
    ResourceAuthorityAdapterResponse,
    ResourceAuthorityAdapterTransportError,
    ResourceAuthorityJournalClient,
    ResourceAuthorityJournalSocketServer,
    ResourceJournalExternalRootReceipt,
    _decode,
    _encode,
    _operation_id,
    _recv_frame,
    _reservation_shell,
    _secure_socket_path,
    _send_frame,
    compose_production_resource_authority_socket_server,
)
from rquant.resource_admission import (
    AdmissionPolicy,
    AdmissionRequest,
    ResourceReservationIdentity,
    ResourceReservationLease,
    ResourceSnapshot,
    TradingSession,
)
from rquant.resource_journal_high_water import (
    RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
    RESOURCE_JOURNAL_HIGH_WATER_PURPOSE,
    TRUSTED_RESOURCE_ROLE_PURPOSES,
    ResourceJournalAntiRollbackReceipt,
    ResourceJournalHighWaterCheckpoint,
    TrustedRoleInventory,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_resource_admission import (
    RESOURCE_OPERATION_KEY_PURPOSE,
    ClosedResourceOperationKeyring,
    ResourceOperationReceipt,
    RuntimeResourceAdmissionCancelledError,
    RuntimeResourceAdmissionError,
    RuntimeResourceAdmissionLockWaitTimeoutError,
    RuntimeResourceAdmissionTransientError,
    SQLiteResourceAdmissionAuthority,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

NOW = datetime(2026, 8, 9, 7, 0, tzinfo=UTC)


class _Signer:
    signature_algorithm = "ed25519"
    issuer = "lab-resource-authority-test"
    key_id = "lab-resource-authority-test-key"
    key_purpose = RESOURCE_OPERATION_KEY_PURPOSE

    def __init__(self) -> None:
        self._secret = b"lab-resource-authority-adapter-test-secret" * 2
        self.public_key_fingerprint = hashlib.sha256(self._secret).hexdigest()

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


def _inventory(signer: _Signer) -> TrustedRoleInventory:
    fingerprints = {
        purpose: frozenset({hashlib.sha256(f"lab-test:{purpose}".encode()).hexdigest()})
        for purpose in TRUSTED_RESOURCE_ROLE_PURPOSES
    }
    fingerprints[RESOURCE_OPERATION_KEY_PURPOSE] = frozenset({signer.public_key_fingerprint})
    fingerprints[RESOURCE_JOURNAL_HIGH_WATER_PURPOSE] = frozenset(
        {hashlib.sha256(b"lab-resource-root-not-configured").hexdigest()}
    )
    return TrustedRoleInventory(role_fingerprints=fingerprints)


def _policy() -> AdmissionPolicy:
    return AdmissionPolicy(
        allow_live_session=True,
        max_live_shard_duration_ms=5_000,
        max_snapshot_age_seconds=30,
        max_live_backlog_age_seconds=30,
        max_live_p95_latency_seconds=30,
        min_available_memory_bytes=0,
        min_available_disk_bytes=0,
        max_io_pressure_pct=100,
        max_cpu_load_pct=100,
        max_expected_memory_bytes=8 * 1024**3,
        max_expected_disk_bytes=8 * 1024**3,
        max_expected_quota_units=0,
        retry_delay_seconds=1,
    )


def _snapshot() -> ResourceSnapshot:
    return ResourceSnapshot(
        observed_at=NOW,
        session=TradingSession.POST_MARKET,
        live_backlog_age_seconds=0,
        live_p95_latency_seconds=0,
        available_memory_bytes=4 * 1024**3,
        available_disk_bytes=16 * 1024**3,
        io_pressure_pct=0,
        cpu_load_pct=0,
        source_quota_remaining=0,
        live_healthy=True,
    )


def _identity() -> ResourceReservationIdentity:
    return ResourceReservationIdentity(
        job_id=UUID("00000000-0000-0000-0000-000000000001"),
        run_id="a" * 64,
        shard_id=UUID("00000000-0000-0000-0000-000000000002"),
        attempt_id=UUID("00000000-0000-0000-0000-000000000003"),
        claim_generation=1,
        scheduler_fencing_token=1,
        worker_id="lab-worker-test",
    )


class _RootVerifier(_Signer):
    issuer = "resource-root-issuer"
    key_id = "resource-root-key"
    key_purpose = RESOURCE_JOURNAL_HIGH_WATER_PURPOSE


def _external_root_config() -> ExternalResourceJournalRootConfig:
    verifier = _RootVerifier()
    return ExternalResourceJournalRootConfig(
        transport="unix-socket-v1",
        transport_manifest_hash="9" * 64,
        root_authority_id="external-root-authority",
        root_store_id="external-root-store",
        root_issuer=verifier.issuer,
        root_key_id=verifier.key_id,
        root_public_key_fingerprint=verifier.public_key_fingerprint,
        witness_rollback_domain_id="external-root-domain",
        local_rollback_domain_id="resource-authority-domain",
    )


class _ExternalRootClient:
    role = "resource_journal_monotonic_root"
    authority_id = "external-root-authority"
    store_id = "external-root-store"
    transport = "unix-socket-v1"
    manifest_hash = "9" * 64
    rollback_domain_id = "external-root-domain"

    def __init__(self) -> None:
        self.calls = 0
        self.last_request = None
        self.response = None
        self.response_factory = None

    def invoke(self, *, request_json: str) -> str | None:
        from rquant.external_monotonic_root import ExternalMonotonicRootRequest

        self.last_request = strict_model_validate_canonical_json(
            ExternalMonotonicRootRequest,
            request_json,
        )
        self.calls += 1
        if self.response_factory is not None:
            return self.response_factory(self.last_request)
        return self.response


def _root_checkpoint() -> ResourceJournalHighWaterCheckpoint:
    signer = _Signer()
    zero_hash = "0" * 64
    head = {
        "authority_id": "resource-authority",
        "lineage_id": "1" * 64,
        "genesis_hash": "2" * 64,
        "keyring_policy_hash": "3" * 64,
        "sequence": 0,
        "entry_hash": zero_hash,
        "previous_head_hash": zero_hash,
        "materialized_state_root": "4" * 64,
        "issuer": signer.issuer,
        "key_id": signer.key_id,
        "key_purpose": signer.key_purpose,
        "namespace": "rquant-resource-admission-journal-head/v1",
        "signature_algorithm": signer.signature_algorithm,
        "public_key_fingerprint": signer.public_key_fingerprint,
        "signature": "signed-head",
    }
    return ResourceJournalHighWaterCheckpoint(
        schema_version=1,
        contract="rquant-resource-journal-high-water-checkpoint/v1",
        journal_authority_id="resource-authority",
        lineage_id="1" * 64,
        sequence=0,
        previous_head_hash=zero_hash,
        head_hash=canonical_sha256(head),
        materialized_state_root="4" * 64,
        signed_head_json=canonical_json_bytes(head).decode("utf-8"),
    )


def _request(identity: ResourceReservationIdentity) -> AdmissionRequest:
    return AdmissionRequest(
        job_id=str(identity.job_id),
        resource_class="standard",
        expected_memory_bytes=1024,
        expected_disk_bytes=1024,
        expected_quota_units=0,
        expected_duration_ms=1_000,
        source=None,
        preemptible=True,
        read_only=True,
        deadline=NOW + timedelta(minutes=10),
    )


class _Server:
    def __init__(
        self,
        tmp_path: Path,
        *,
        filename: str = "resource.sqlite3",
        timeout_milliseconds: int = 1_000,
        policy_provider: Callable[[], AdmissionPolicy] = _policy,
    ) -> None:
        tmp_path.chmod(0o700)
        socket_parent = Path(__file__).resolve().parents[2] / ".s"
        socket_parent.mkdir(parents=True, exist_ok=True)
        socket_parent.chmod(0o700)
        self.socket_root = Path(tempfile.mkdtemp(prefix="rqa-", dir=socket_parent))
        self.socket_root.chmod(0o700)
        self.signer = _Signer()
        self.inventory = _inventory(self.signer)
        self.authority = SQLiteResourceAdmissionAuthority(
            tmp_path / filename,
            authority_id="lab-resource-authority-test",
            signer=self.signer,
            keyring=ClosedResourceOperationKeyring(
                verifiers=(self.signer,),
                trusted_issuer=self.signer.issuer,
                trusted_role_inventory=self.inventory,
            ),
            mode="test-standalone",
            clock=lambda: NOW,
        )
        self.configuration = ResourceAuthorityAdapterConfig(
            mode="test-standalone",
            endpoint=self.socket_root / "resource.sock",
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            authority_id=self.authority.authority_id,
            trusted_role_inventory_hash=self.inventory.policy_hash,
            timeout_milliseconds=timeout_milliseconds,
        )
        self.server = ResourceAuthorityJournalSocketServer(
            configuration=self.configuration,
            authority=self.authority,
            policy_provider=policy_provider,
            snapshot_provider=_snapshot,
        )
        self.listener = self.server.bind()
        self.listener.settimeout(0.05)
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self) -> None:
        while not self.stop.is_set():
            try:
                self.server.serve_once(self.listener)
            except TimeoutError:
                continue
            except OSError:
                if not self.stop.is_set():
                    raise

    def close(self) -> None:
        self.stop.set()
        self.listener.close()
        self.thread.join(timeout=2)
        self.configuration.endpoint.unlink(missing_ok=True)
        self.socket_root.rmdir()


def _spawn_policy_round_trip(configuration_json: str, result_path: str) -> None:
    configuration = ResourceAuthorityAdapterConfig.model_validate_json(
        configuration_json, strict=True
    )
    policy = ResourceAuthorityJournalClient(configuration).policy(operation_id="spawn-policy")
    Path(result_path).write_bytes(_encode(policy))


def _spawn_worker_release_after_restart(
    configuration_json: str,
    identity_json: str,
    lease_json: str,
    runtime_root: str,
    result_path: str,
) -> None:
    from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
    from rquant.lab_worker import LabWorker, build_resource_journal_authority_manifest

    configuration = ResourceAuthorityAdapterConfig.model_validate_json(
        configuration_json, strict=True
    )
    identity = ResourceReservationIdentity.model_validate_json(identity_json, strict=True)
    lease = ResourceReservationLease.model_validate_json(lease_json, strict=True)
    root = Path(runtime_root)
    worker = LabWorker(
        worker_id=identity.worker_id,
        claim_spool=LabClaimSpool(root / "claims"),
        report_spool=LabReportSpool(root / "reports"),
        artifact_root=root / "artifacts",
        resource_authority_manifest=build_resource_journal_authority_manifest(configuration),
        require_resource_admission=True,
        verified_code_sha_provider=lambda: "1" * 40,
    )
    store = worker.resource_reservation_store
    if not isinstance(store, LabResourceAuthorityReservationAdapter):
        raise RuntimeError("worker did not construct the registered resource authority")
    released = store.release(lease, identity=identity)
    Path(result_path).write_bytes(canonical_json_bytes({"released": released}))


def _client(server: _Server) -> ResourceAuthorityJournalClient:
    return ResourceAuthorityJournalClient(server.configuration)


def test_resource_socket_path_rejects_a_writable_ancestor(tmp_path: Path) -> None:
    socket_parent = Path(__file__).resolve().parents[2] / ".s"
    socket_parent.mkdir(parents=True, exist_ok=True)
    socket_parent.chmod(0o700)
    short_root = Path(tempfile.mkdtemp(prefix="s-", dir=socket_parent)).resolve()
    unsafe = short_root / "u"
    socket_root = unsafe / "r"
    socket_root.mkdir(parents=True, mode=0o700)
    unsafe.chmod(0o770)
    endpoint = socket_root / "s"
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(str(endpoint))
        endpoint.chmod(0o600)
        with pytest.raises(ResourceAuthorityAdapterTransportError, match="endpoint"):
            _secure_socket_path(
                endpoint,
                expected_uid=os.getuid(),
                expected_gid=os.getgid(),
                expected_mode=0o600,
            )
    finally:
        listener.close()
        endpoint.unlink(missing_ok=True)
        socket_root.rmdir()
        unsafe.chmod(0o700)
        unsafe.rmdir()
        short_root.rmdir()


def test_config_requires_an_explicit_external_root_for_production(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="anti-rollback root"):
        ResourceAuthorityAdapterConfig(
            mode="production",
            endpoint=Path("/tmp/rqa.sock"),
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            authority_id="resource-authority",
            trusted_role_inventory_hash="a" * 64,
        )

    standalone = ResourceAuthorityAdapterConfig(
        mode="test-standalone",
        endpoint=Path("/tmp/rqa.sock"),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        authority_id="resource-authority",
        trusted_role_inventory_hash="a" * 64,
    )
    assert standalone.non_production is True

    root = _external_root_config()
    production = ResourceAuthorityAdapterConfig(
        mode="production",
        endpoint=Path("/tmp/rqa.sock"),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        expected_server_uid=os.getuid() + 1,
        expected_server_gid=os.getgid() + 1,
        allowed_peer_uid=os.getuid() + 2,
        allowed_peer_gid=os.getgid() + 2,
        socket_mode=0o660,
        socket_directory_mode=0o750,
        authority_id="resource-authority",
        high_water_authority_id="resource-high-water-authority",
        external_root_config=root,
        trusted_role_inventory_hash="a" * 64,
    )
    assert production.external_root_config == root
    assert production.expected_server_identity == (os.getuid() + 1, os.getgid() + 1)
    assert production.allowed_peer_identity == (os.getuid() + 2, os.getgid() + 2)

    with pytest.raises(ValueError, match="registered external root transport"):
        ResourceAuthorityAdapterConfig(
            mode="production",
            endpoint=Path("/tmp/rqa.sock"),
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            authority_id="resource-authority",
            high_water_authority_id="resource-high-water-authority",
            external_root_config=ExternalResourceJournalRootConfig(
                **{
                    **root.model_dump(mode="python"),
                    "transport": "nonproduction-inprocess-v1",
                }
            ),
            trusted_role_inventory_hash="a" * 64,
        )

    with pytest.raises(ValueError, match="independent rollback domain"):
        ExternalResourceJournalRootConfig.model_validate(
            {
                **root.model_dump(mode="python"),
                "local_rollback_domain_id": "external-root-domain",
            },
            strict=True,
        )


def test_production_server_requires_callable_external_root_capability(tmp_path: Path) -> None:
    root = _external_root_config()
    config = ResourceAuthorityAdapterConfig(
        mode="production",
        endpoint=Path("/tmp/rqa.sock"),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        authority_id="resource-authority",
        high_water_authority_id="resource-high-water-authority",
        external_root_config=root,
        trusted_role_inventory_hash="a" * 64,
    )
    authority = SimpleNamespace(
        authority_id="resource-authority",
        mode="production",
        trusted_role_inventory_hash="a" * 64,
        high_water_authority=SimpleNamespace(
            mode="production",
            authority_id="resource-high-water-authority",
            anti_rollback_root_authority_id=root.root_authority_id,
            verifier_fingerprints=frozenset({root.root_public_key_fingerprint}),
        ),
    )
    with pytest.raises(ResourceAuthorityAdapterConfigurationError, match="root client"):
        ResourceAuthorityJournalSocketServer(
            configuration=config,
            authority=authority,
            policy_provider=_policy,
            snapshot_provider=_snapshot,
        )

    missing_capability = SimpleNamespace(
        role=_ExternalRootClient.role,
        authority_id=_ExternalRootClient.authority_id,
        store_id=_ExternalRootClient.store_id,
        transport=_ExternalRootClient.transport,
        manifest_hash=_ExternalRootClient.manifest_hash,
        rollback_domain_id=_ExternalRootClient.rollback_domain_id,
    )
    with pytest.raises(ResourceAuthorityAdapterConfigurationError, match="closed Unix peer"):
        compose_production_resource_authority_socket_server(
            configuration=config,
            authority=authority,
            policy_provider=_policy,
            snapshot_provider=_snapshot,
            external_root_client=missing_capability,
            external_root_verifiers=(_RootVerifier(),),
        )

    structural_fake = _ExternalRootClient()
    with pytest.raises(
        ResourceAuthorityAdapterConfigurationError,
        match="closed Unix peer client",
    ):
        compose_production_resource_authority_socket_server(
            configuration=config,
            authority=authority,
            policy_provider=_policy,
            snapshot_provider=_snapshot,
            external_root_client=structural_fake,
            external_root_verifiers=(_RootVerifier(),),
        )

    nonproduction = ExternalResourceJournalMonotonicRootAdapter.for_nonproduction_test(
        config=root,
        client=structural_fake,
        root_verifiers=(_RootVerifier(),),
    )
    assert nonproduction.production_ready is False


def test_resource_authority_probe_contract_is_canonical_and_closed() -> None:
    adapter_identity = ResourceAuthorityAdapterIdentity(
        mode="test-standalone",
        authority_id="resource-authority",
        trusted_role_inventory_hash="a" * 64,
    )
    request = ResourceAuthorityAdapterRequest(
        operation="probe",
        operation_id="probe-operation",
    )
    response = ResourceAuthorityAdapterResponse(
        operation="probe",
        identity=adapter_identity,
        capabilities=("policy", "snapshot", "journal"),
    )

    assert (
        _decode(
            _encode(request),
            model=ResourceAuthorityAdapterRequest,
            label="probe request",
        )
        == request
    )
    assert (
        _decode(
            _encode(response),
            model=ResourceAuthorityAdapterResponse,
            label="probe response",
        )
        == response
    )
    with pytest.raises(ValueError, match="probe response"):
        ResourceAuthorityAdapterResponse(
            operation="probe",
            identity=adapter_identity,
            capabilities=("snapshot", "policy", "journal"),
        )


def test_external_resource_root_adapter_uses_generic_runtime_and_verifies_receipt() -> None:
    config = _external_root_config()
    client = _ExternalRootClient()
    verifier = _RootVerifier()
    root = ExternalResourceJournalMonotonicRootAdapter.for_nonproduction_test(
        config=config,
        client=client,
        root_verifiers=(verifier,),
    )
    checkpoint = _root_checkpoint()
    unsigned = ResourceJournalAntiRollbackReceipt(
        schema_version=1,
        contract="rquant-resource-journal-anti-rollback-receipt/v1",
        root_authority_id=config.root_authority_id,
        high_water_authority_id="resource-high-water-authority",
        journal_authority_id="resource-authority",
        operation_id="5" * 64,
        previous_checkpoint_hash="0" * 64,
        checkpoint=checkpoint,
        issuer=verifier.issuer,
        key_id=verifier.key_id,
        key_purpose=verifier.key_purpose,
        namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
        signature_algorithm=verifier.signature_algorithm,
        public_key_fingerprint=verifier.public_key_fingerprint,
        signature="pending",
    )
    receipt = unsigned.model_copy(
        update={
            "signature": verifier.sign(
                namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
                payload=unsigned.signing_bytes(),
            )
        }
    )

    def signed_response(request: object) -> str:
        assert request is not None
        external_unsigned = ResourceJournalExternalRootReceipt(
            schema_version=1,
            contract="rquant-resource-journal-external-root-receipt/v1",
            role=config.role,
            root_authority_id=config.root_authority_id,
            root_store_id=config.root_store_id,
            journal_authority_id="resource-authority",
            request_kind=request.kind,
            request_hash=request.request_hash,
            challenge_nonce=request.challenge_nonce,
            receipt=receipt,
            issuer=verifier.issuer,
            key_id=verifier.key_id,
            key_purpose=verifier.key_purpose,
            namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
            signature_algorithm=verifier.signature_algorithm,
            public_key_fingerprint=verifier.public_key_fingerprint,
            signature="pending",
        )
        external_receipt = external_unsigned.model_copy(
            update={
                "signature": verifier.sign(
                    namespace=RESOURCE_JOURNAL_ANTI_ROLLBACK_RECEIPT_NAMESPACE,
                    payload=external_unsigned.signing_bytes(),
                )
            }
        )
        return canonical_model_json_bytes(external_receipt).decode("utf-8")

    client.response_factory = signed_response

    assert (
        root.pin(
            operation_id="5" * 64,
            high_water_authority_id="resource-high-water-authority",
            journal_authority_id="resource-authority",
            checkpoint=checkpoint,
        )
        == receipt
    )
    assert client.last_request is not None
    assert client.last_request.kind == "pin"


def test_server_refuses_to_replace_a_non_socket_endpoint(tmp_path: Path) -> None:
    server = _Server(tmp_path)
    endpoint = server.configuration.endpoint
    socket_root = server.socket_root
    server.close()
    socket_root.mkdir(mode=0o700)
    endpoint.write_text("not a socket", encoding="utf-8")
    try:
        with pytest.raises(ResourceAuthorityAdapterConfigurationError, match="cannot be safely"):
            server.server.bind()
    finally:
        endpoint.unlink(missing_ok=True)
        socket_root.rmdir()


def test_server_refuses_a_symlinked_socket_parent(tmp_path: Path) -> None:
    server = _Server(tmp_path)
    socket_root = server.socket_root
    server.close()
    replacement = tmp_path / "untrusted-socket-parent"
    replacement.mkdir(mode=0o700)
    socket_root.symlink_to(replacement, target_is_directory=True)
    try:
        with pytest.raises(ResourceAuthorityAdapterConfigurationError, match="not private"):
            server.server.bind()
    finally:
        socket_root.unlink(missing_ok=True)
        replacement.rmdir()


def test_real_spawn_child_uses_only_frozen_config_and_canonical_bytes(tmp_path: Path) -> None:
    server = _Server(tmp_path)
    try:
        output = tmp_path / "spawn-policy.json"
        context = multiprocessing.get_context("spawn")
        process = context.Process(
            target=_spawn_policy_round_trip,
            args=(server.configuration.model_dump_json(), str(output)),
        )
        process.start()
        process.join(timeout=10)
        assert process.exitcode == 0
        assert AdmissionPolicy.model_validate_json(output.read_bytes(), strict=True) == _policy()
    finally:
        server.close()


def test_registry_v2_binding_is_closed_and_unknown_versions_fail_closed(tmp_path: Path) -> None:
    server = _Server(tmp_path)
    try:
        from rquant.lab_worker import (
            LabClosedRegistryBinding,
            LabResourceAuthorityManifest,
            _AuthorityWireRequest,
            _resolve_authority,
            build_resource_journal_authority_manifest,
        )

        manifest = build_resource_journal_authority_manifest(server.configuration)
        assert manifest.registry.registry_id == LAB_RESOURCE_AUTHORITY_REGISTRY_ID
        assert manifest.registry.registry_version == LAB_RESOURCE_AUTHORITY_REGISTRY_VERSION
        assert manifest.registry.registry_hash == LAB_RESOURCE_AUTHORITY_REGISTRY_HASH
        assert (
            _resolve_authority(
                _AuthorityWireRequest(operation="admission", manifest=manifest)
            ).policy
            == _policy()
        )

        unknown = LabResourceAuthorityManifest(
            registry=LabClosedRegistryBinding(
                registry_id="rquant.lab-authority.unknown",
                registry_version=999,
                registry_hash="b" * 64,
                configuration_json="{}",
            )
        )
        with pytest.raises(Exception, match="not registered"):
            _resolve_authority(_AuthorityWireRequest(operation="policy", manifest=unknown))
    finally:
        server.close()


def test_adapter_round_trip_response_loss_retry_restart_and_terminal_receipts(
    tmp_path: Path,
) -> None:
    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        server.server.drop_next_response_after_effect_for_test("reserve")
        with pytest.raises(ResourceAuthorityAdapterTransportError):
            adapter.reserve(
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
            )
        admitted = adapter.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None
        receipt = server.authority.lookup(
            _operation_id(operation="reserve", identity=identity, prior=None)
        ).receipt
        assert isinstance(receipt, ResourceOperationReceipt)
        renewed = adapter.recheck(
            lease=admitted.lease,
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert renewed.lease is not None
        assert adapter.release(renewed.lease, identity=identity) is True
    finally:
        server.close()

    restarted = _Server(tmp_path)
    try:
        assert _client(restarted).lookup(operation_id=receipt.operation_id).receipt == receipt
    finally:
        restarted.close()


def test_adapter_restart_recovers_latest_receipt_and_terminal_state(tmp_path: Path) -> None:
    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        first = LabResourceAuthorityReservationAdapter(server.configuration)
        reserved = first.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert reserved.lease is not None
        server.server.drop_next_response_after_effect_for_test("recheck")
        with pytest.raises(ResourceAuthorityAdapterTransportError):
            first.recheck(
                lease=reserved.lease,
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
            )
        latest_after_loss = _client(server).lookup_latest(
            identity=identity,
            lease_id=reserved.lease.lease_id,
        )

        restarted = LabResourceAuthorityReservationAdapter(server.configuration)
        recovered = restarted.recheck(
            lease=reserved.lease,
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert recovered.lease is not None
        latest_after_retry = _client(server).lookup_latest(
            identity=identity,
            lease_id=reserved.lease.lease_id,
        )
        assert latest_after_retry.receipt == latest_after_loss.receipt
        assert recovered.lease == latest_after_loss.lease

        terminal = LabResourceAuthorityReservationAdapter(server.configuration)
        server.server.drop_next_response_after_effect_for_test("release")
        with pytest.raises(ResourceAuthorityAdapterTransportError):
            terminal.release(reserved.lease, identity=identity)
        after_terminal_restart = LabResourceAuthorityReservationAdapter(server.configuration)
        assert after_terminal_restart.release(reserved.lease, identity=identity) is True
        with pytest.raises(RuntimeResourceAdmissionError, match="terminal"):
            after_terminal_restart.recheck(
                lease=reserved.lease,
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
            )
    finally:
        server.close()


def test_worker_restart_constructs_adapter_that_recovers_authority_lease(
    tmp_path: Path,
) -> None:
    from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
    from rquant.lab_worker import LabWorker, build_resource_journal_authority_manifest

    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    manifest = build_resource_journal_authority_manifest(server.configuration)

    def worker(name: str) -> LabWorker:
        return LabWorker(
            worker_id="lab-worker-test",
            claim_spool=LabClaimSpool(tmp_path / f"{name}-claims"),
            report_spool=LabReportSpool(tmp_path / f"{name}-reports"),
            artifact_root=tmp_path / f"{name}-artifacts",
            resource_authority_manifest=manifest,
            require_resource_admission=True,
            verified_code_sha_provider=lambda: "1" * 40,
        )

    try:
        first_store = worker("first").resource_reservation_store
        assert isinstance(first_store, LabResourceAuthorityReservationAdapter)
        admitted = first_store.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None
        server.server.drop_next_response_after_effect_for_test("recheck")
        with pytest.raises(ResourceAuthorityAdapterTransportError):
            first_store.recheck(
                lease=admitted.lease,
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
            )

        restarted_store = worker("restarted").resource_reservation_store
        assert isinstance(restarted_store, LabResourceAuthorityReservationAdapter)
        assert restarted_store.release(admitted.lease, identity=identity) is True
    finally:
        server.close()


def test_spawned_worker_restart_recovers_lost_recheck_and_releases(
    tmp_path: Path,
) -> None:
    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    first = LabResourceAuthorityReservationAdapter(server.configuration)
    try:
        admitted = first.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None
        server.server.drop_next_response_after_effect_for_test("recheck")
        with pytest.raises(ResourceAuthorityAdapterTransportError):
            first.recheck(
                lease=admitted.lease,
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
            )

        runtime_root = tmp_path / "spawned-worker-restart"
        runtime_root.mkdir(mode=0o700)
        result_path = tmp_path / "spawned-worker-release.json"
        process = multiprocessing.get_context("spawn").Process(
            target=_spawn_worker_release_after_restart,
            args=(
                server.configuration.model_dump_json(),
                identity.model_dump_json(),
                admitted.lease.model_dump_json(),
                str(runtime_root),
                str(result_path),
            ),
        )
        process.start()
        process.join(timeout=10)

        assert process.exitcode == 0
        assert result_path.read_bytes() == b'{"released":true}'
        latest = _client(server).lookup_latest(
            identity=identity,
            lease_id=admitted.lease.lease_id,
        )
        assert latest.released is True
        assert latest.lease is None
    finally:
        server.close()


def test_adapter_concurrent_double_release_is_idempotent(tmp_path: Path) -> None:
    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        admitted = LabResourceAuthorityReservationAdapter(server.configuration).reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None

        def release_from_restart() -> bool:
            return LabResourceAuthorityReservationAdapter(server.configuration).release(
                admitted.lease,
                identity=identity,
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = tuple(
                future.result(timeout=5)
                for future in (
                    executor.submit(release_from_restart),
                    executor.submit(release_from_restart),
                )
            )
        assert outcomes == (True, True)
    finally:
        server.close()


def test_stale_fence_donor_identity_and_concurrent_terminal_operations_fail_closed(
    tmp_path: Path,
) -> None:
    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        client = _client(server)
        reserved = client.reserve(
            operation_id="reserve-operation",
            identity=identity,
            request=request,
            lease_seconds=30,
        )
        assert reserved.lease is not None
        renewed = client.recheck(
            operation_id="recheck-operation",
            lease=reserved.lease,
            identity=identity,
            request=request,
            lease_seconds=30,
            prior_receipt=reserved.receipt,
        )
        assert renewed.lease is not None
        with pytest.raises(ResourceAuthorityAdapterRemoteError):
            client.release(
                operation_id="stale-release-operation",
                lease=renewed.lease,
                identity=identity,
                prior_receipt=reserved.receipt,
            )

        def terminal(operation_id: str) -> object:
            try:
                return client.release(
                    operation_id=operation_id,
                    lease=renewed.lease,
                    identity=identity,
                    prior_receipt=renewed.receipt,
                )
            except ResourceAuthorityAdapterRemoteError as exc:
                return exc

        with ThreadPoolExecutor(max_workers=2) as executor:
            outcomes = tuple(
                future.result(timeout=5)
                for future in (
                    executor.submit(terminal, "release-a"),
                    executor.submit(terminal, "release-b"),
                )
            )
        assert sum(not isinstance(value, Exception) for value in outcomes) == 1
        assert (
            sum(isinstance(value, ResourceAuthorityAdapterRemoteError) for value in outcomes) == 1
        )
    finally:
        server.close()


def test_tamper_unknown_duplicate_nan_and_oversize_frames_fail_closed(tmp_path: Path) -> None:
    server = _Server(tmp_path)
    try:
        wrong_identity = server.configuration.model_copy(update={"authority_id": "donor-authority"})
        with pytest.raises(ResourceAuthorityAdapterTransportError, match="identity"):
            ResourceAuthorityJournalClient(wrong_identity).policy(operation_id="tamper")

        from rquant.lab_resource_authority_adapter import ResourceAuthorityAdapterRequest

        with pytest.raises(ResourceAuthorityAdapterTransportError):
            _decode(
                b'{"message_type":"resource-authority-request","message_type":"x"}',
                model=ResourceAuthorityAdapterRequest,
                label="duplicate",
            )
        with pytest.raises(ResourceAuthorityAdapterTransportError):
            _decode(
                b'{"message_type":"resource-authority-request","operation":"policy",'
                b'"operation_id":"x","schema_version":NaN}',
                model=ResourceAuthorityAdapterRequest,
                label="nan",
            )
        with pytest.raises(ResourceAuthorityAdapterTransportError):
            _decode(
                b"{" + b"x" * RESOURCE_AUTHORITY_ADAPTER_MAX_WIRE_BYTES,
                model=ResourceAuthorityAdapterRequest,
                label="oversize",
            )
    finally:
        server.close()


@contextmanager
def _authority_write_lock(authority: SQLiteResourceAdmissionAuthority) -> Iterator[None]:
    """Hold the authority's real SQLite write lock for the duration of the block."""

    holder = sqlite3.connect(authority.path, isolation_level=None)
    try:
        holder.execute("BEGIN IMMEDIATE")
        yield
    finally:
        with suppress(sqlite3.Error):  # the lock was never taken
            holder.execute("ROLLBACK")
        holder.close()


class _LockingPolicyProvider:
    """Take the write lock exactly when the server enters a mutation.

    ``_handle`` resolves the policy before it calls the authority, so arming
    this seam pins the contention inside ``reserve``/``recheck`` itself - after
    the adapter's recovery lookup already succeeded - with no timing race.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self.armed = False
        self.locked = threading.Event()
        self._holder: sqlite3.Connection | None = None

    def __call__(self) -> AdmissionPolicy:
        if self.armed and self._holder is None:
            holder = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
            holder.execute("BEGIN IMMEDIATE")
            self._holder = holder
            self.locked.set()
        return _policy()

    def close(self) -> None:
        holder = self._holder
        self._holder = None
        if holder is not None:
            with suppress(sqlite3.Error):  # already rolled back
                holder.execute("ROLLBACK")
            holder.close()


def _record_handled(server: _Server) -> list[ResourceAuthorityAdapterRequest]:
    seen: list[ResourceAuthorityAdapterRequest] = []
    original = server.server._handle

    def recording(
        request: ResourceAuthorityAdapterRequest,
    ) -> ResourceAuthorityAdapterResponse:
        seen.append(request)
        return original(request)

    server.server._handle = recording  # type: ignore[method-assign]
    return seen


def _raw_round_trip(
    server: _Server,
    request: ResourceAuthorityAdapterRequest,
    *,
    timeout_seconds: float = 10.0,
) -> ResourceAuthorityAdapterResponse:
    """Speak the wire directly so the server's own classification is observable."""

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(timeout_seconds)
        connection.connect(str(server.configuration.endpoint))
        _send_frame(connection, _encode(request))
        raw = _recv_frame(connection, label="raw resource authority response")
    decoded = _decode(raw, model=ResourceAuthorityAdapterResponse, label="raw response")
    assert isinstance(decoded, ResourceAuthorityAdapterResponse)
    return decoded


def _reserve_wire_request(
    identity: ResourceReservationIdentity,
    request: AdmissionRequest,
    **overrides: object,
) -> ResourceAuthorityAdapterRequest:
    return ResourceAuthorityAdapterRequest(
        operation="reserve",
        operation_id=_operation_id(operation="reserve", identity=identity, prior=None),
        identity=identity,
        admission_request=request,
        lease=_reservation_shell(identity=identity, request=request),
        lease_seconds=30,
        **overrides,
    )


def test_adapter_reserve_stops_at_the_callers_remaining_lock_wait_budget(
    tmp_path: Path,
) -> None:
    """20ms of caller budget must not turn into a full 1000ms socket wait.

    The adapter used to open with ``del lock_wait_timeout_seconds``, so the
    caller's remaining tick/deadline budget never reached either the socket or
    the authority (issue #159 / RQ-CTB-P1-01).
    """

    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        with _authority_write_lock(server.authority):
            started = time.monotonic()
            with pytest.raises(RuntimeResourceAdmissionTransientError) as caught:
                adapter.reserve(
                    identity=identity,
                    request=request,
                    policy=_policy(),
                    snapshot_provider=_snapshot,
                    lease_seconds=30,
                    lock_wait_timeout_seconds=0.02,
                )
            elapsed = time.monotonic() - started
        assert isinstance(caught.value, RuntimeResourceAdmissionLockWaitTimeoutError)
        assert elapsed < server.configuration.timeout_milliseconds / 1_000 / 2
        assert server.authority.active_leases() == ()
    finally:
        server.close()


def test_adapter_recheck_stops_at_the_callers_remaining_lock_wait_budget(
    tmp_path: Path,
) -> None:
    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        admitted = adapter.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None
        with _authority_write_lock(server.authority):
            started = time.monotonic()
            with pytest.raises(RuntimeResourceAdmissionTransientError) as caught:
                adapter.recheck(
                    lease=admitted.lease,
                    identity=identity,
                    request=request,
                    policy=_policy(),
                    snapshot_provider=_snapshot,
                    lease_seconds=30,
                    lock_wait_timeout_seconds=0.02,
                )
            elapsed = time.monotonic() - started
        assert isinstance(caught.value, RuntimeResourceAdmissionLockWaitTimeoutError)
        assert elapsed < server.configuration.timeout_milliseconds / 1_000 / 2
    finally:
        server.close()


def test_contended_authority_keeps_transient_contention_typed_on_the_wire(
    tmp_path: Path,
) -> None:
    """The server must publish a structured retryable kind, not a bare refusal.

    ``error_code`` alone collapsed a lost race and a broken contract into one
    ``ResourceAuthorityAdapterRemoteError``, which ``lab_worker`` then folded
    into ``LabDaemonConfigurationError``.
    """

    server = _Server(tmp_path, timeout_milliseconds=5_000)
    identity = _identity()
    request = _request(identity)
    try:
        wire_request = _reserve_wire_request(
            identity,
            request,
            lock_wait_timeout_milliseconds=20,
        )
        with _authority_write_lock(server.authority):
            started = time.monotonic()
            response = _raw_round_trip(server, wire_request)
            elapsed = time.monotonic() - started
        assert response.error_code == "authority"
        assert response.error_kind == "lock_wait_timeout"
        assert response.result is None
        assert elapsed < 0.5
        assert server.authority.active_leases() == ()
    finally:
        server.close()


def test_authority_serves_a_request_that_carries_no_lock_wait_budget(
    tmp_path: Path,
) -> None:
    """The new field is optional: an unset budget keeps the server's own default."""

    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        wire_request = _reserve_wire_request(identity, request)
        assert wire_request.lock_wait_timeout_milliseconds is None
        response = _raw_round_trip(server, wire_request)
        assert response.error_code is None
        assert response.error_kind is None
        assert response.result is not None
        assert response.result.lease is not None
    finally:
        server.close()


def test_lock_wait_budget_is_validated_by_the_closed_request_contract() -> None:
    identity = _identity()
    request = _request(identity)
    for rejected in (0, -1, 1_001):
        with pytest.raises(ValidationError):
            _reserve_wire_request(identity, request, lock_wait_timeout_milliseconds=rejected)
    with pytest.raises(ValueError, match="lock wait budget"):
        ResourceAuthorityAdapterRequest(
            operation="policy",
            operation_id="policy-operation",
            lock_wait_timeout_milliseconds=20,
        )
    accepted = _reserve_wire_request(identity, request, lock_wait_timeout_milliseconds=1_000)
    assert accepted.lock_wait_timeout_milliseconds == 1_000
    # The wire stays closed: a payload that simply omits the key is not canonical.
    truncated = _encode(accepted).replace(b'"lock_wait_timeout_milliseconds":1000,', b"")
    with pytest.raises(ResourceAuthorityAdapterTransportError):
        _decode(truncated, model=ResourceAuthorityAdapterRequest, label="legacy request")


def test_error_kind_is_derived_from_typed_classes_and_restored_as_one() -> None:
    """The retry semantics survive the wire without a message or sqlite code."""

    from rquant.lab_resource_authority_adapter import _authority_error_kind, _remote_refusal

    assert (
        _authority_error_kind(RuntimeResourceAdmissionLockWaitTimeoutError("x"))
        == "lock_wait_timeout"
    )
    assert _authority_error_kind(RuntimeResourceAdmissionTransientError("x")) == "transient"
    assert _authority_error_kind(RuntimeResourceAdmissionCancelledError("x")) == "cancelled"
    assert _authority_error_kind(RuntimeResourceAdmissionError("x")) == "contract"
    assert _authority_error_kind(ValueError("x")) == "contract"

    lock_wait = _remote_refusal("lock_wait_timeout")
    assert isinstance(lock_wait, RuntimeResourceAdmissionLockWaitTimeoutError)
    transient = _remote_refusal("transient")
    assert isinstance(transient, RuntimeResourceAdmissionTransientError)
    assert not isinstance(transient, RuntimeResourceAdmissionLockWaitTimeoutError)
    cancelled = _remote_refusal("cancelled")
    assert isinstance(cancelled, RuntimeResourceAdmissionCancelledError)
    # A stop is not a lost race: retrying it is the wrong answer.
    assert not isinstance(cancelled, RuntimeResourceAdmissionTransientError)
    for absent in ("contract", None):
        refusal = _remote_refusal(absent)
        assert type(refusal) is ResourceAuthorityAdapterRemoteError
        assert not isinstance(refusal, RuntimeResourceAdmissionError)


def test_lock_wait_budget_ceiling_matches_the_store() -> None:
    """The wire ceiling is the store's own ceiling, so a request can only shorten."""

    from rquant.runtime_resource_admission import _MAX_RESOURCE_LOCK_WAIT_SECONDS

    assert RESOURCE_AUTHORITY_MAX_LOCK_WAIT_MILLISECONDS == _MAX_RESOURCE_LOCK_WAIT_SECONDS * 1_000


def test_adapter_recheck_carries_the_budget_into_a_contended_server_mutation(
    tmp_path: Path,
) -> None:
    """Contention inside the mutation itself stays retryable end to end."""

    gate = _LockingPolicyProvider(tmp_path / "resource.sqlite3")
    server = _Server(tmp_path, timeout_milliseconds=5_000, policy_provider=gate)
    identity = _identity()
    request = _request(identity)
    try:
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        admitted = adapter.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None
        seen = _record_handled(server)
        gate.armed = True
        started = time.monotonic()
        with pytest.raises(RuntimeResourceAdmissionTransientError) as caught:
            adapter.recheck(
                lease=admitted.lease,
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=0.5,
            )
        elapsed = time.monotonic() - started
        assert isinstance(caught.value, RuntimeResourceAdmissionLockWaitTimeoutError)
        assert gate.locked.is_set()
        assert elapsed < 2.0
        rechecks = [entry for entry in seen if entry.operation == "recheck"]
        assert len(rechecks) == 1
        budget = rechecks[0].lock_wait_timeout_milliseconds
        assert budget is not None and 0 < budget <= 500
    finally:
        gate.close()
        server.close()


def test_adapter_release_carries_the_caller_budget_to_the_authority(
    tmp_path: Path,
) -> None:
    """`release()` had a bare constant on the caller side and dropped it here."""

    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        admitted = adapter.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None
        seen = _record_handled(server)
        assert adapter.release(admitted.lease, identity=identity, lock_wait_timeout_seconds=0.5)
        releases = [entry for entry in seen if entry.operation == "release"]
        assert len(releases) == 1
        budget = releases[0].lock_wait_timeout_milliseconds
        assert budget is not None and 0 < budget <= 500
        lookups = [entry for entry in seen if entry.operation == "lookup-latest"]
        # The fenced recovery lookup takes no server budget: the authority's
        # `lookup_latest` accepts none (issue #163 C), so the wire stays honest.
        assert lookups and all(entry.lock_wait_timeout_milliseconds is None for entry in lookups)
    finally:
        server.close()


def test_adapter_reserve_answers_a_stop_authority_before_and_during_the_wait(
    tmp_path: Path,
) -> None:
    server = _Server(tmp_path, timeout_milliseconds=5_000)
    identity = _identity()
    request = _request(identity)
    try:
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        seen = _record_handled(server)
        with pytest.raises(RuntimeResourceAdmissionCancelledError):
            adapter.reserve(
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=1.0,
                stop_requested=lambda: True,
            )
        assert seen == []

        polls: list[bool] = []

        def stop_once_the_server_is_handling() -> bool:
            # The seam is the request's arrival, not a clock: the flip can only
            # be observed by a poll taken while the response is in flight.
            stopped = bool(seen)
            polls.append(stopped)
            return stopped

        with _authority_write_lock(server.authority):
            started = time.monotonic()
            with pytest.raises(RuntimeResourceAdmissionCancelledError):
                adapter.reserve(
                    identity=identity,
                    request=request,
                    policy=_policy(),
                    snapshot_provider=_snapshot,
                    lease_seconds=30,
                    lock_wait_timeout_seconds=1.0,
                    stop_requested=stop_once_the_server_is_handling,
                )
            elapsed = time.monotonic() - started
        assert polls.count(False) >= 2
        assert polls[-1] is True
        assert elapsed < 0.5
    finally:
        server.close()


def test_adapter_recheck_answers_a_stop_authority_before_and_during_the_wait(
    tmp_path: Path,
) -> None:
    server = _Server(tmp_path, timeout_milliseconds=5_000)
    identity = _identity()
    request = _request(identity)
    try:
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        admitted = adapter.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
        )
        assert admitted.lease is not None
        seen = _record_handled(server)
        with pytest.raises(RuntimeResourceAdmissionCancelledError):
            adapter.recheck(
                lease=admitted.lease,
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=1.0,
                stop_requested=lambda: True,
            )
        assert seen == []

        polls: list[bool] = []

        def stop_once_the_server_is_handling() -> bool:
            stopped = bool(seen)
            polls.append(stopped)
            return stopped

        with _authority_write_lock(server.authority):
            started = time.monotonic()
            with pytest.raises(RuntimeResourceAdmissionCancelledError):
                adapter.recheck(
                    lease=admitted.lease,
                    identity=identity,
                    request=request,
                    policy=_policy(),
                    snapshot_provider=_snapshot,
                    lease_seconds=30,
                    lock_wait_timeout_seconds=1.0,
                    stop_requested=stop_once_the_server_is_handling,
                )
            elapsed = time.monotonic() - started
        assert polls.count(False) >= 2
        assert polls[-1] is True
        assert elapsed < 0.5
    finally:
        server.close()


# --- WP9b: the connect/send phases poll the stop authority (design §5.2/§5.3) ---
#
# Every case below drives the module-level ``_socket_factory`` seam.  The seam
# is a *test* seam: production still constructs the same AF_UNIX socket and
# ``ResourceAuthorityJournalClient`` grew no constructor field for it.


def _unix_socket() -> socket.socket:
    return socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)


class _SeamSocket:
    """A recording stand-in installed through ``_socket_factory``.

    Anything the test does not script delegates to a real socket, so the
    adapter keeps speaking the real wire; only the calls a case needs to bend
    (``connect_ex`` return codes, ``send`` return values) are intercepted.
    Every scripted hook is *bounded*: a missing stop poll or a missing
    zero-progress guard has to fail the case, never hang the suite.
    """

    def __init__(
        self,
        inner: socket.socket,
        *,
        connect_codes: Callable[[int], int | None] | None = None,
        send_bytes: Callable[[int], int | None] | None = None,
    ) -> None:
        self._inner = inner
        self._connect_codes = connect_codes
        self._send_bytes = send_bytes
        self.connect_attempts = 0
        self.blocking_connects = 0
        self.send_calls = 0
        self.sendall_calls = 0
        self.sent_bytes = 0
        self.first_chunk_bytes = 0
        self.send_timeouts = 0

    def __enter__(self) -> _SeamSocket:
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self._inner.close()

    def __getattr__(self, name: str) -> object:
        inner = self.__dict__.get("_inner")
        if inner is None:  # pragma: no cover - only during a failed __init__
            raise AttributeError(name)
        return getattr(inner, name)

    def connect(self, address: object) -> None:
        self.blocking_connects += 1
        self._inner.connect(address)

    def connect_ex(self, address: object) -> int:
        attempt = self.connect_attempts
        self.connect_attempts += 1
        if self._connect_codes is None:
            return self._inner.connect_ex(address)
        code = self._connect_codes(attempt)
        if code is None:
            return self._inner.connect_ex(address)
        return code

    def send(self, data: object) -> int:
        self.send_calls += 1
        if self.send_calls == 1:
            self.first_chunk_bytes = len(memoryview(data))  # type: ignore[arg-type]
        if self._send_bytes is not None:
            scripted = self._send_bytes(self.send_calls - 1)
            if scripted is not None:
                self.sent_bytes += scripted
                return scripted
        try:
            sent = self._inner.send(data)
        except TimeoutError:
            self.send_timeouts += 1
            raise
        self.sent_bytes += sent
        return sent

    def sendall(self, data: object) -> None:
        self.sendall_calls += 1
        self._inner.sendall(data)


def _install_socket_seam(
    monkeypatch: pytest.MonkeyPatch,
    build: Callable[[], _SeamSocket],
) -> list[_SeamSocket]:
    """Point the module seam at ``build`` and collect every socket it made."""

    made: list[_SeamSocket] = []

    def factory() -> _SeamSocket:
        seam = build()
        made.append(seam)
        return seam

    monkeypatch.setattr(adapter_module, "_socket_factory", factory)
    return made


def _stalling_connect_codes(*, attempts_before_refusal: int) -> Callable[[int], int | None]:
    """Answer EAGAIN - Linux's AF_UNIX "backlog is full" - a bounded number of times.

    The bound is the case's own guard rail: with the stop/deadline poll in
    place the loop never reaches it, and without the poll the case fails on a
    wrong exception type instead of spinning forever.
    """

    def codes(attempt: int) -> int | None:
        if attempt < attempts_before_refusal:
            return errno.EAGAIN
        return errno.ECONNREFUSED

    return codes


def test_wp9b_connect_answers_a_stop_authority_within_one_poll_period(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2-1: a stop raised while connecting is answered on the next poll.

    Before WP9b the whole ``connect`` was one blocking call bounded only by
    ``max(connect_seconds, poll)``, so a stop raised during it was invisible
    until the connection either landed or timed out.
    """

    server = _Server(tmp_path, timeout_milliseconds=5_000)
    identity = _identity()
    request = _request(identity)
    try:
        seen = _record_handled(server)
        made = _install_socket_seam(
            monkeypatch,
            lambda: _SeamSocket(
                _unix_socket(),
                connect_codes=_stalling_connect_codes(attempts_before_refusal=64),
            ),
        )

        def stop_after_the_first_connect_attempt() -> bool:
            # The seam is the attempt itself, not a clock: the flip can only be
            # observed by a poll taken between two connect attempts.
            return bool(made) and made[0].connect_attempts >= 1

        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        started = time.monotonic()
        with pytest.raises(RuntimeResourceAdmissionCancelledError):
            adapter.reserve(
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=1.0,
                stop_requested=stop_after_the_first_connect_attempt,
            )
        elapsed = time.monotonic() - started
        assert len(made) == 1
        assert made[0].connect_attempts == 1
        assert made[0].send_calls == 0
        assert made[0].sendall_calls == 0
        assert seen == []
        assert elapsed < server.configuration.timeout_milliseconds / 1_000 / 10
    finally:
        server.close()


def test_wp9b_connect_charges_an_exhausted_budget_to_the_caller(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2-3 (connect phase): budget exhaustion is retryable, not a deployment fault.

    This is the classification change §5.2 point 5 requires to be declared:
    the poll inside the connect loop reclassifies an exhausted caller budget
    from ``ResourceAuthorityAdapterTransportError`` to
    ``RuntimeResourceAdmissionLockWaitTimeoutError``.
    """

    server = _Server(tmp_path, timeout_milliseconds=5_000)
    identity = _identity()
    request = _request(identity)
    try:
        seen = _record_handled(server)
        made = _install_socket_seam(
            monkeypatch,
            lambda: _SeamSocket(
                _unix_socket(),
                connect_codes=_stalling_connect_codes(attempts_before_refusal=4_096),
            ),
        )
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        started = time.monotonic()
        with pytest.raises(RuntimeResourceAdmissionLockWaitTimeoutError) as caught:
            adapter.reserve(
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=0.05,
            )
        elapsed = time.monotonic() - started
        assert not isinstance(caught.value, ResourceAuthorityAdapterTransportError)
        assert made[0].connect_attempts >= 1
        assert made[0].send_calls == 0
        assert seen == []
        assert elapsed < server.configuration.timeout_milliseconds / 1_000 / 10
    finally:
        server.close()


def test_wp9b_a_round_trip_without_a_budget_keeps_one_blocking_connect_and_sendall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2-4: no budget means no waiter, and no waiter means the old shape.

    ``waiter is None`` still takes the single blocking ``connect`` and the
    single ``sendall`` - neither the non-blocking connect loop nor the chunked
    send may appear on that path.
    """

    server = _Server(tmp_path)
    try:
        made = _install_socket_seam(monkeypatch, lambda: _SeamSocket(_unix_socket()))
        policy = _client(server).policy(operation_id="wp9b-no-budget")
        assert policy == _policy()
        assert len(made) == 1
        assert made[0].blocking_connects == 1
        assert made[0].connect_attempts == 0
        assert made[0].sendall_calls == 1
        assert made[0].send_calls == 0
    finally:
        server.close()


def test_wp9b_connect_remembers_success_when_a_second_connect_would_answer_eagain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2-6: Linux answers EAGAIN on an *already connected* AF_UNIX socket.

    Measured on Linux 3.11.16/3.12.14 (run 33584787985): with the listener's
    backlog full, ``connect()`` on a connected socket answers ``EAGAIN``, not
    ``EISCONN``.  Success therefore has to be remembered by the loop; probing
    for ``EISCONN`` would spin until the caller's budget expired.
    """

    server = _Server(tmp_path)
    identity = _identity()
    request = _request(identity)
    try:
        made = _install_socket_seam(
            monkeypatch,
            lambda: _SeamSocket(
                _unix_socket(),
                connect_codes=lambda attempt: None if attempt == 0 else errno.EAGAIN,
            ),
        )
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        admitted = adapter.reserve(
            identity=identity,
            request=request,
            policy=_policy(),
            snapshot_provider=_snapshot,
            lease_seconds=30,
            lock_wait_timeout_seconds=1.0,
        )
        assert admitted.lease is not None
        # `reserve` plus the fenced `lookup-latest`, one connect attempt each.
        assert len(made) == 2
        assert [seam.connect_attempts for seam in made] == [1, 1]
    finally:
        server.close()


def _full_peer_pair() -> tuple[socket.socket, socket.socket]:
    """An AF_UNIX pair whose peer receive buffer is full.

    The adapter builds its own client socket inside the round trip, so the
    client's ``SO_SNDBUF`` is only reachable through the seam - which is why
    this pair is handed to ``_socket_factory`` rather than dialled.  Both
    directions are shrunk and then filled from the client end in small pieces,
    so a later drain of one piece frees a bounded, sub-frame window on either
    kernel (macOS accounts by ``SO_SNDLOWAT``, Linux by whole skbs).
    """

    client, peer = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    with suppress(OSError):  # a kernel is free to clamp to its own floor
        client.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 512)
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 512)
    client.setblocking(False)
    filled = 0
    with suppress(BlockingIOError):
        while filled < 1 << 20:
            filled += client.send(b"\0" * 512)
    assert filled > 0, "the pair never accepted a byte"
    client.setblocking(True)
    return client, peer


def _open_a_sub_frame_window(client: socket.socket, peer: socket.socket) -> int:
    """Drain the least the kernel needs before it calls ``client`` writable.

    Draining in small steps and stopping at the first writable poll keeps the
    freed window far below one frame, so the send that follows makes partial
    progress and the one after it blocks again.
    """

    drained = 0
    while drained < 2_048:
        drained += len(peer.recv(128))
        if select.select((), (client,), (), 0)[1]:
            break
    return drained


def test_wp9b_send_answers_a_stop_authority_between_two_chunks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2-2: a stop raised mid-frame is answered on the next poll.

    ``_send_frame`` used to be one ``sendall`` carrying the socket timeout that
    was set for *connect*, so a full peer parked the caller there with no stop
    poll and no record of how much had already reached the wire.
    """

    server = _Server(tmp_path, timeout_milliseconds=5_000)
    identity = _identity()
    request = _request(identity)
    client, peer = _full_peer_pair()
    try:
        seen = _record_handled(server)
        made: list[_SeamSocket] = []

        def open_a_window_before_the_second_chunk(call_index: int) -> int | None:
            if call_index == 1:
                _open_a_sub_frame_window(client, peer)
            return None  # never script the return value: the kernel answers

        made = _install_socket_seam(
            monkeypatch,
            lambda: _SeamSocket(
                client,
                connect_codes=lambda _attempt: 0,  # the pair is already connected
                send_bytes=open_a_window_before_the_second_chunk,
            ),
        )

        def stop_once_a_chunk_reached_the_wire() -> bool:
            # The seam is the partial write, not a clock: only a poll taken
            # between two chunks can observe it.
            return bool(made) and made[0].sent_bytes > 0

        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        started = time.monotonic()
        with pytest.raises(RuntimeResourceAdmissionCancelledError):
            adapter.reserve(
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=1.0,
                stop_requested=stop_once_a_chunk_reached_the_wire,
            )
        elapsed = time.monotonic() - started
        assert len(made) == 1
        seam = made[0]
        assert seam.sendall_calls == 0
        assert seam.send_timeouts >= 1, "the full peer never blocked the send"
        assert seam.sent_bytes > 0, "the offset never advanced"
        assert seam.sent_bytes < seam.first_chunk_bytes, "the whole frame left before the stop"
        assert seen == []
        assert elapsed < server.configuration.timeout_milliseconds / 1_000 / 10
    finally:
        peer.close()
        client.close()
        server.close()


def test_wp9b_send_charges_an_exhausted_budget_to_the_caller(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2-3 (send phase): the same classification change, on the other half."""

    server = _Server(tmp_path, timeout_milliseconds=5_000)
    identity = _identity()
    request = _request(identity)
    client, peer = _full_peer_pair()
    try:
        seen = _record_handled(server)
        made = _install_socket_seam(
            monkeypatch,
            lambda: _SeamSocket(client, connect_codes=lambda _attempt: 0),
        )
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        started = time.monotonic()
        with pytest.raises(RuntimeResourceAdmissionLockWaitTimeoutError) as caught:
            adapter.reserve(
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=0.05,
            )
        elapsed = time.monotonic() - started
        assert not isinstance(caught.value, ResourceAuthorityAdapterTransportError)
        assert made[0].send_timeouts >= 2
        assert seen == []
        assert elapsed < server.configuration.timeout_milliseconds / 1_000 / 10
    finally:
        peer.close()
        client.close()
        server.close()


def test_wp9b_a_send_that_makes_no_progress_fails_typed_instead_of_looping(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """P2-5: ``send`` returning 0 must end the frame, not repeat it forever.

    No platform probe ever saw a blocking AF_UNIX ``send`` answer 0; this is
    the guard rail for the offset that cannot advance, so the case bounds
    itself and fails loudly rather than hanging if the guard is gone.
    """

    server = _Server(tmp_path, timeout_milliseconds=5_000)
    identity = _identity()
    request = _request(identity)
    try:
        seen = _record_handled(server)

        def never_makes_progress(call_index: int) -> int:
            assert call_index < 8, "the zero-progress guard is missing"
            return 0

        made = _install_socket_seam(
            monkeypatch,
            lambda: _SeamSocket(_unix_socket(), send_bytes=never_makes_progress),
        )
        adapter = LabResourceAuthorityReservationAdapter(server.configuration)
        with pytest.raises(ResourceAuthorityAdapterTransportError, match="made no progress"):
            adapter.reserve(
                identity=identity,
                request=request,
                policy=_policy(),
                snapshot_provider=_snapshot,
                lease_seconds=30,
                lock_wait_timeout_seconds=1.0,
            )
        assert made[0].send_calls == 1
        assert made[0].sendall_calls == 0
        assert seen == []
    finally:
        server.close()
