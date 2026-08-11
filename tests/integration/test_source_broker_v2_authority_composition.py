from __future__ import annotations

import copy
import hashlib
import inspect
import os
import shutil
import socket
import sqlite3
import stat
import subprocess
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Literal

import pytest

import rquant.external_monotonic_root as external_root_module
import rquant.external_monotonic_root_service as root_service_module
import rquant.source_broker_v2_authority as authority_module
import rquant.source_broker_v2_authority_service as authority_service_module
import rquant.source_broker_v2_runner as runner_module
import rquant.source_broker_v2_runtime as runtime_module
from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    SecureCreatedFile,
    SecurePathMetadata,
)
from rquant.lab_source_stage import LabSourceStageStore
from rquant.source_broker_protocol import PeerCredentialsPolicy, SocketEndpointIdentity
from rquant.source_broker_v2 import SourceBrokerV2Saga
from rquant.source_broker_v2_authority import (
    SourceBrokerV2AuthorityCompositionError,
    compose_production_source_broker_v2_authorities,
    compose_production_source_broker_v2_current_claim_service,
    compose_production_source_broker_v2_replay_lineage_service,
    compose_production_source_broker_v2_root_service,
    compose_production_source_broker_v2_scheduler_clients,
    compose_production_source_broker_v2_source_daemon_policy,
    compose_production_source_broker_v2_source_quota_service,
    compose_production_source_broker_v2_source_signer,
)
from rquant.source_broker_v2_runtime import (
    SourceBrokerV2AuthorityRuntime,
    SourceBrokerV2IdentityMatrix,
    SourceBrokerV2ProcessIdentity,
    SourceBrokerV2ProcessRole,
    SourceBrokerV2RootRole,
    source_broker_v2_default_runtime,
)
from rquant.source_quota_broker_adapter import SourceQuotaParentBindingV2
from rquant.source_quota_store import SourceQuotaStore
from tests.unit.test_adapter_manifest import create_test_authorities


def _stage_bound_runner(
    *,
    db_path: Path,
    registry: runner_module.SourceBrokerV2StaticProviderRegistry,
    config: runner_module.SourceBrokerV2JobRunnerConfig,
) -> runner_module.SourceBrokerV2JobRunner:
    runner_module.initialize_source_broker_v2_job_storage(db_path, busy_timeout_ms=5_000)
    authority_set = create_test_authorities(db_path.parent / f"{db_path.stem}-intent-keys")
    keyring = authority_set.authorization_keyring
    stage_store = LabSourceStageStore(
        db_path.with_name(f"{db_path.stem}.source-stage.sqlite3"),
        queue_store_path=db_path,
        manifest_keyring=keyring,
        authorization_keyring=keyring,
    )
    return runner_module.SourceBrokerV2JobRunner(
        db_path=db_path,
        registry=registry,
        config=config,
        manifest_keyring=keyring,
        authorization_keyring=keyring,
        stage_store=stage_store,
    )


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for SourceBroker v2 authority composition")
    return executable


def _identity(
    role: SourceBrokerV2ProcessRole,
    *,
    uid: int,
    gid: int,
) -> SourceBrokerV2ProcessIdentity:
    return SourceBrokerV2ProcessIdentity(role=role, uid=uid, gid=gid)


def _identities() -> SourceBrokerV2IdentityMatrix:
    role = SourceBrokerV2ProcessRole
    return SourceBrokerV2IdentityMatrix(
        current_claim=_identity(role.CURRENT_CLAIM_AUTHORITY, uid=51_001, gid=61_001),
        source_quota=_identity(role.SOURCE_QUOTA_AUTHORITY, uid=51_002, gid=61_002),
        replay_lineage=_identity(role.REPLAY_LINEAGE_AUTHORITY, uid=51_003, gid=61_003),
        current_claim_root=_identity(
            role.CURRENT_CLAIM_ROOT_SERVICE,
            uid=51_004,
            gid=61_001,
        ),
        source_quota_root=_identity(
            role.SOURCE_QUOTA_ROOT_SERVICE,
            uid=51_005,
            gid=61_002,
        ),
        replay_lineage_root=_identity(
            role.REPLAY_LINEAGE_ROOT_SERVICE,
            uid=51_006,
            gid=61_003,
        ),
        source_daemon=_identity(role.SOURCE_DAEMON, uid=51_007, gid=61_004),
        scheduler_client=_identity(
            role.SCHEDULER_SOURCE_CLIENT,
            uid=51_008,
            gid=61_004,
        ),
    )


class _ThreadIdentityOs:
    def __init__(self, runtime: SourceBrokerV2AuthorityRuntime) -> None:
        self._runtime = runtime
        self._active: SourceBrokerV2ProcessIdentity | None = None
        self.read_paths: list[Path] = []

    def geteuid(self) -> int:
        identity = self._thread_identity()
        return os.geteuid() if identity is None else identity.uid

    def getegid(self) -> int:
        identity = self._thread_identity()
        return os.getegid() if identity is None else identity.gid

    def chown(self, _path: Path, uid: int, gid: int) -> None:
        identity = self._thread_identity()
        if identity is None or uid != identity.uid or gid < 1:
            raise PermissionError("logical authority chown identity changed")

    @contextmanager
    def as_identity(self, identity: SourceBrokerV2ProcessIdentity) -> Iterator[None]:
        previous = self._active
        self._active = identity
        try:
            yield
        finally:
            self._active = previous

    def __getattr__(self, name: str) -> object:
        return getattr(os, name)

    def _thread_identity(self) -> SourceBrokerV2ProcessIdentity | None:
        prefix = "source-broker-v2-root:"
        name = threading.current_thread().name
        if name.startswith(prefix):
            return self._runtime.root(SourceBrokerV2RootRole(name.removeprefix(prefix))).identity
        authority_prefix = "source-broker-v2-authority:"
        if name.startswith(authority_prefix):
            role = SourceBrokerV2ProcessRole(name.removeprefix(authority_prefix))
            return {
                SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY: (
                    self._runtime.current_claim.identity
                ),
                SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY: (
                    self._runtime.source_quota.identity
                ),
                SourceBrokerV2ProcessRole.REPLAY_LINEAGE_AUTHORITY: (
                    self._runtime.replay_lineage.identity
                ),
            }[role]
        return self._active


def _generate_key_pair(private_key: Path, public_key: Path) -> None:
    subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)),
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    subprocess.run(
        (_openssl(), "pkey", "-in", str(private_key), "-pubout", "-out", str(public_key)),
        check=True,
        capture_output=True,
    )
    public_key.chmod(0o600)


def _copy_public_key(source: Path, destination: Path) -> None:
    destination.write_bytes(source.read_bytes())
    destination.chmod(0o600)


def _short_socket_runtime(
    runtime: SourceBrokerV2AuthorityRuntime,
    *,
    root: Path,
) -> SourceBrokerV2AuthorityRuntime:
    token = hashlib.sha256(os.fspath(root).encode()).hexdigest()[:10]
    socket_base = Path(__file__).resolve().parents[2] / ".s" / token
    root_names = {
        SourceBrokerV2RootRole.CURRENT_CLAIM: "c",
        SourceBrokerV2RootRole.SOURCE_QUOTA: "q",
        SourceBrokerV2RootRole.REPLAY_LINEAGE: "l",
    }
    roots = tuple(
        external_root.model_copy(
            update={
                "run_directory": socket_base / root_names[external_root.role],
                "socket_path": socket_base / root_names[external_root.role] / "root.sock",
            }
        )
        for external_root in runtime.roots
    )
    source_daemon = runtime.source_daemon.model_copy(
        update={
            "run_directory": socket_base / "s",
            "socket_path": socket_base / "s" / "source-authority.sock",
        }
    )
    current_claim = runtime.current_claim.model_copy(
        update={
            "run_directory": socket_base / "a",
            "socket_path": socket_base / "a" / "authority.sock",
        }
    )
    source_quota = runtime.source_quota.model_copy(
        update={
            "run_directory": socket_base / "u",
            "socket_path": socket_base / "u" / "authority.sock",
        }
    )
    replay_lineage = runtime.replay_lineage.model_copy(
        update={
            "run_directory": socket_base / "y",
            "socket_path": socket_base / "y" / "authority.sock",
        }
    )
    return SourceBrokerV2AuthorityRuntime.model_validate(
        runtime.model_dump(mode="python")
        | {
            "roots": roots,
            "source_daemon": source_daemon,
            "current_claim": current_claim,
            "source_quota": source_quota,
            "replay_lineage": replay_lineage,
        },
        strict=True,
    )


def _install_logical_identity_environment(
    runtime: SourceBrokerV2AuthorityRuntime,
    monkeypatch: pytest.MonkeyPatch,
) -> _ThreadIdentityOs:
    logical_os = _ThreadIdentityOs(runtime)
    logical_owners = {
        policy.path: (policy.owner_uid, policy.owner_gid)
        for policy in (*runtime.protected_directories, *runtime.protected_files)
    }
    for root in runtime.roots:
        logical_owners[root.socket_path] = (root.identity.uid, root.identity.gid)
    for authority in (
        runtime.current_claim,
        runtime.source_quota,
        runtime.replay_lineage,
    ):
        logical_owners[authority.socket_path] = (
            authority.identity.uid,
            runtime.identities.scheduler_client.gid,
        )
    logical_owners[runtime.source_daemon.socket_path] = (
        runtime.source_daemon.identity.uid,
        runtime.source_daemon.identity.gid,
    )

    def secure_metadata(
        path: Path,
        *,
        trusted_root: Path = Path("/"),
        allowed_ancestor_uids: frozenset[int] | None = None,
        kind: Literal["directory", "file", "socket"],
        expected_uid: int,
        expected_gid: int,
        expected_mode: int,
    ) -> SecurePathMetadata:
        del trusted_root, allowed_ancestor_uids
        candidate = Path(path)
        try:
            observed = candidate.lstat()
        except OSError as exc:
            raise AuthorityPathSecurityError("logical protected path is unavailable") from exc
        expected_kind = {
            "directory": stat.S_ISDIR,
            "file": stat.S_ISREG,
            "socket": stat.S_ISSOCK,
        }[kind]
        declared_owner = logical_owners.get(candidate)
        actual_owner = (observed.st_uid, observed.st_gid)
        if (
            not expected_kind(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or (kind == "file" and observed.st_nlink != 1)
            or stat.S_IMODE(observed.st_mode) != expected_mode
            or (expected_uid, expected_gid) not in {declared_owner, actual_owner}
        ):
            raise AuthorityPathSecurityError(
                "logical protected path owner, mode, or inode is unsafe"
            )
        rebound = candidate.lstat()
        if (observed.st_dev, observed.st_ino) != (rebound.st_dev, rebound.st_ino):
            raise AuthorityPathSecurityError("logical protected path identity changed")
        return SecurePathMetadata(
            uid=expected_uid,
            gid=expected_gid,
            mode=stat.S_IMODE(observed.st_mode),
            device=observed.st_dev,
            inode=observed.st_ino,
            size=observed.st_size,
        )

    def read_secure_file(
        path: Path,
        *,
        trusted_root: Path = Path("/"),
        allowed_ancestor_uids: frozenset[int] | None = None,
        expected_uid: int,
        expected_gid: int,
        allowed_final_uids: frozenset[int] | None = None,
        allowed_final_gids: frozenset[int] | None = None,
        allowed_modes: frozenset[int],
        max_bytes: int,
    ) -> bytes:
        if 0o600 not in allowed_modes:
            raise AuthorityPathSecurityError("test key policy does not allow private mode")
        declared_owner = logical_owners.get(Path(path))
        final_uid = expected_uid
        final_gid = expected_gid
        if declared_owner is not None:
            if allowed_final_uids is not None and declared_owner[0] in allowed_final_uids:
                final_uid = declared_owner[0]
            if allowed_final_gids is not None and declared_owner[1] in allowed_final_gids:
                final_gid = declared_owner[1]
        metadata = secure_metadata(
            path,
            trusted_root=trusted_root,
            allowed_ancestor_uids=allowed_ancestor_uids,
            kind="file",
            expected_uid=final_uid,
            expected_gid=final_gid,
            expected_mode=0o600,
        )
        if metadata.size > max_bytes:
            raise AuthorityPathSecurityError("logical protected file is too large")
        logical_os.read_paths.append(Path(path))
        return Path(path).read_bytes()

    def secure_create_file(
        path: Path,
        *,
        trusted_root: Path = Path("/"),
        allowed_ancestor_uids: frozenset[int] | None = None,
        expected_uid: int,
        expected_gid: int,
        expected_mode: int,
    ) -> SecureCreatedFile:
        candidate = Path(path)
        created = not candidate.exists()
        candidate.touch(mode=expected_mode, exist_ok=True)
        candidate.chmod(expected_mode)
        return SecureCreatedFile(
            metadata=secure_metadata(
                candidate,
                trusted_root=trusted_root,
                allowed_ancestor_uids=allowed_ancestor_uids,
                kind="file",
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                expected_mode=expected_mode,
            ),
            created=created,
        )

    sockets = {root.socket_path: root for root in runtime.roots}

    def peer_credentials(connection: socket.socket) -> tuple[int, int]:
        local = connection.getsockname()
        if local:
            root = sockets[Path(os.fsdecode(local))]
            return root.consumer_identity.uid, root.consumer_identity.gid
        peer = connection.getpeername()
        root = sockets[Path(os.fsdecode(peer))]
        return root.service_identity.uid, root.service_identity.gid

    def validate_socket_path(client: object) -> None:
        manifest = client._manifest  # type: ignore[attr-defined]
        secure_metadata(
            manifest.socket_path,
            kind="socket",
            expected_uid=manifest.socket_uid,
            expected_gid=manifest.socket_gid,
            expected_mode=manifest.socket_mode,
        )

    def validate_server_peer(client: object, connection: socket.socket) -> None:
        del connection
        manifest = client._manifest  # type: ignore[attr-defined]
        root = sockets[manifest.socket_path]
        if (manifest.peer_uid, manifest.peer_gid) != (
            root.service_identity.uid,
            root.service_identity.gid,
        ):
            raise authority_module.ExternalMonotonicRootSecurityError(
                "external root Unix peer identity changed"
            )

    authority_sockets = {
        authority.socket_path: authority
        for authority in (
            runtime.current_claim,
            runtime.source_quota,
            runtime.replay_lineage,
        )
    }

    def validate_authority_endpoint(
        policy: object,
        *,
        expected_identity: SocketEndpointIdentity | None = None,
    ) -> SocketEndpointIdentity:
        if policy.path == runtime.source_daemon.socket_path:
            expected_owner = logical_owners[policy.path]
            if (
                policy.owner_uid,
                policy.group_gid,
            ) != expected_owner or policy.mode != runtime.source_daemon.socket_mode:
                raise AuthorityPathSecurityError("logical source endpoint changed")
            identity = SocketEndpointIdentity(
                device=1,
                inode=1,
                owner_uid=policy.owner_uid,
                group_gid=policy.group_gid,
                mode=policy.mode,
            )
        else:
            metadata = secure_metadata(
                policy.path,
                kind="socket",
                expected_uid=policy.owner_uid,
                expected_gid=policy.group_gid,
                expected_mode=policy.mode,
            )
            identity = SocketEndpointIdentity(
                device=metadata.device,
                inode=metadata.inode,
                owner_uid=metadata.uid,
                group_gid=metadata.gid,
                mode=metadata.mode,
            )
        if expected_identity is not None and identity != expected_identity:
            raise authority_service_module.SourceBrokerV2AuthorityServiceError(
                "logical authority endpoint changed"
            )
        return identity

    def authority_peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
        local = connection.getsockname()
        if local:
            scheduler = runtime.identities.scheduler_client
            return 9001, scheduler.uid, scheduler.gid
        peer = Path(os.fsdecode(connection.getpeername()))
        identity = authority_sockets[peer].identity
        return 9002, identity.uid, identity.gid

    monkeypatch.setattr(runtime_module, "secure_path_metadata", secure_metadata)
    monkeypatch.setattr(authority_module, "secure_path_metadata", secure_metadata)
    monkeypatch.setattr(authority_module, "read_secure_regular_file", read_secure_file)
    monkeypatch.setattr(authority_module, "secure_create_regular_file", secure_create_file)
    monkeypatch.setattr(authority_module, "os", logical_os)
    monkeypatch.setattr(root_service_module, "secure_path_metadata", secure_metadata)
    monkeypatch.setattr(root_service_module, "read_secure_regular_file", read_secure_file)
    monkeypatch.setattr(root_service_module, "secure_create_regular_file", secure_create_file)
    monkeypatch.setattr(root_service_module, "_peer_credentials", peer_credentials)
    monkeypatch.setattr(root_service_module, "os", logical_os)
    monkeypatch.setattr(authority_service_module, "os", logical_os)
    monkeypatch.setattr(
        authority_service_module,
        "validate_socket_parent",
        lambda _policy: None,
    )
    monkeypatch.setattr(
        authority_service_module,
        "validate_socket_endpoint",
        validate_authority_endpoint,
    )
    monkeypatch.setattr(
        runner_module,
        "validate_socket_endpoint",
        validate_authority_endpoint,
    )
    monkeypatch.setattr(
        authority_service_module,
        "_kernel_peer_credentials",
        authority_peer_credentials,
    )
    monkeypatch.setattr(
        authority_service_module,
        "verify_connected_server_authority",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        external_root_module.UnixSocketExternalMonotonicRootClient,
        "_validate_socket_path",
        validate_socket_path,
    )
    monkeypatch.setattr(
        external_root_module.UnixSocketExternalMonotonicRootClient,
        "_validate_peer",
        validate_server_peer,
    )
    return logical_os


def _runtime(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[SourceBrokerV2AuthorityRuntime, _ThreadIdentityOs]:
    runtime = _short_socket_runtime(
        source_broker_v2_default_runtime(root=root, identities=_identities()),
        root=root,
    )
    for directory in runtime.protected_directories:
        directory.path.mkdir(parents=True, mode=directory.mode, exist_ok=True)
        directory.path.chmod(directory.mode)

    initialized_state_paths = (
        runtime.current_claim.state_path,
        runtime.source_quota.state_path,
        runtime.replay_lineage.state_path,
        runtime.scheduler_client.saga_state_path,
        runtime.scheduler_client.quota_adapter_state_path,
        runtime.scheduler_client.source_ledger_state_path,
        *runtime.replay_lineage.separation_state_paths,
    )
    for state_path in initialized_state_paths:
        state_path.touch(mode=0o600)
        state_path.chmod(0o600)

    for private_key, public_key in runtime.authority_key_pairs:
        _generate_key_pair(private_key, public_key)
    for external_root in runtime.roots:
        _copy_public_key(external_root.public_key_path, external_root.consumer_public_key_path)
    _copy_public_key(
        runtime.current_claim.public_key_path,
        runtime.scheduler_client.current_claim_public_key_path,
    )
    _copy_public_key(
        runtime.replay_lineage.public_key_path,
        runtime.scheduler_client.replay_lineage_public_key_path,
    )
    _copy_public_key(
        runtime.source_daemon.public_key_path,
        runtime.scheduler_client.source_current_public_key_path,
    )
    _copy_public_key(
        runtime.source_daemon.next_public_key_path,
        runtime.scheduler_client.source_next_public_key_path,
    )

    test_private_directory = root / "test-only-key-provisioning"
    test_private_directory.mkdir(mode=0o700)
    test_private_directory.chmod(0o700)
    _generate_key_pair(
        test_private_directory / "manifest.private.pem",
        runtime.scheduler_client.manifest_public_key_path,
    )
    manifest_copy = runtime.current_claim.manifest_public_key_path
    assert manifest_copy is not None
    _copy_public_key(runtime.scheduler_client.manifest_public_key_path, manifest_copy)
    logical_os = _install_logical_identity_environment(runtime, monkeypatch)
    return runtime, logical_os


def _start_root_services(
    runtime: SourceBrokerV2AuthorityRuntime,
    logical_os: _ThreadIdentityOs,
) -> list[tuple[object, threading.Thread, threading.Event]]:
    started: list[tuple[object, threading.Thread, threading.Event]] = []
    for role in SourceBrokerV2RootRole:
        with logical_os.as_identity(runtime.root(role).identity):
            service = compose_production_source_broker_v2_root_service(runtime, role=role)
        stop = threading.Event()
        thread = threading.Thread(
            target=service.serve_forever,
            kwargs={"stop": stop},
            daemon=True,
            name=f"source-broker-v2-root:{role.value}",
        )
        thread.start()
        assert service.ready.wait(timeout=5)
        started.append((service, thread, stop))
    return started


def _stop_root_services(started: list[tuple[object, threading.Thread, threading.Event]]) -> None:
    for service, _thread, stop in started:
        stop.set()
        service.wake()  # type: ignore[union-attr]
    for _service, thread, _stop in started:
        thread.join(timeout=5)


def _start_authority_services(
    runtime: SourceBrokerV2AuthorityRuntime,
    logical_os: _ThreadIdentityOs,
) -> list[tuple[object, threading.Thread]]:
    specifications = (
        (
            runtime.current_claim,
            compose_production_source_broker_v2_current_claim_service,
        ),
        (
            runtime.source_quota,
            compose_production_source_broker_v2_source_quota_service,
        ),
        (
            runtime.replay_lineage,
            compose_production_source_broker_v2_replay_lineage_service,
        ),
    )
    started: list[tuple[object, threading.Thread]] = []
    for layout, factory in specifications:
        with logical_os.as_identity(layout.identity):
            service = factory(runtime)
        thread = threading.Thread(
            target=service.serve_forever,
            name=f"source-broker-v2-authority:{layout.identity.role.value}",
        )
        thread.start()
        assert service.wait_ready(timeout=5.0)
        started.append((service, thread))
    return started


def _stop_authority_services(started: list[tuple[object, threading.Thread]]) -> None:
    for service, _thread in started:
        service.shutdown()
    for _service, thread in started:
        thread.join(timeout=5)
        assert not thread.is_alive()


def test_composition_exports_typed_source_daemon_peer_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _logical_os = _runtime(tmp_path, monkeypatch)
    policy = compose_production_source_broker_v2_source_daemon_policy(runtime)

    assert type(policy) is PeerCredentialsPolicy
    assert policy.allowed_uids == frozenset({runtime.identities.scheduler_client.uid})
    assert policy.allowed_gids == frozenset({runtime.identities.scheduler_client.gid})
    assert policy.allows(
        pid=1,
        uid=runtime.identities.scheduler_client.uid,
        gid=runtime.identities.scheduler_client.gid,
    )
    assert not policy.allows(
        pid=1,
        uid=runtime.identities.current_claim.uid,
        gid=runtime.identities.current_claim.gid,
    )


def test_root_services_pin_the_exact_authority_consumer_peer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)

    for role in SourceBrokerV2RootRole:
        root = runtime.root(role)
        with logical_os.as_identity(root.identity):
            service = compose_production_source_broker_v2_root_service(runtime, role=role)
        configuration = service.configuration
        assert (configuration.service_uid, configuration.service_gid) == (
            root.service_identity.uid,
            root.service_identity.gid,
        )
        assert (configuration.allowed_peer_uid, configuration.allowed_peer_gid) == (
            root.consumer_identity.uid,
            root.consumer_identity.gid,
        )
        assert configuration.socket_mode == 0o660
        assert configuration.socket_directory_mode == 0o750


def test_role_local_services_and_public_only_scheduler_clients_restart_cleanly(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    current_root = runtime.root(SourceBrokerV2RootRole.CURRENT_CLAIM)
    runtime.current_claim.state_path.unlink()
    with logical_os.as_identity(current_root.identity):
        compose_production_source_broker_v2_root_service(
            runtime,
            role=SourceBrokerV2RootRole.CURRENT_CLAIM,
        )
    partial_root_inode = current_root.state_path.stat().st_ino
    with pytest.raises(SourceBrokerV2AuthorityCompositionError, match="aggregate.*forbidden"):
        compose_production_source_broker_v2_authorities(runtime)

    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        assert current_root.state_path.stat().st_ino == partial_root_inode
        with pytest.raises(SourceBrokerV2AuthorityCompositionError, match="aggregate.*forbidden"):
            compose_production_source_broker_v2_authorities(runtime)
        authorities = _start_authority_services(runtime, logical_os)
        logical_os.read_paths.clear()
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
        scheduler_key_paths = {
            runtime.scheduler_client.manifest_public_key_path,
            runtime.scheduler_client.current_claim_public_key_path,
            runtime.scheduler_client.replay_lineage_public_key_path,
            runtime.scheduler_client.source_current_public_key_path,
            runtime.scheduler_client.source_next_public_key_path,
        }
        assert set(logical_os.read_paths) == scheduler_key_paths
        assert not set(logical_os.read_paths) & {
            policy.path
            for policy in runtime.protected_files
            if policy.purpose in {"state", "private-key"}
        }
        assert clients.current_claim.preflight().non_production is False
        assert clients.source_quota.preflight().accepted is True
        assert clients.replay_lineage.preflight().non_production is False
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)

    assert clients.source_authority_keyring.allowed_key_ids == (
        runtime.source_authority_current_key_id,
        runtime.source_authority_next_key_id,
    )
    current_authority_inode = runtime.current_claim.state_path.stat().st_ino

    restarted_roots = _start_root_services(runtime, logical_os)
    restarted_authorities: list[tuple[object, threading.Thread]] = []
    try:
        restarted_authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            restarted_clients = compose_production_source_broker_v2_scheduler_clients(runtime)
        assert restarted_clients.current_claim.preflight().non_production is False
        assert current_root.state_path.stat().st_ino == partial_root_inode
        assert runtime.current_claim.state_path.stat().st_ino == current_authority_inode
    finally:
        _stop_authority_services(restarted_authorities)
        _stop_root_services(restarted_roots)

    assert restarted_clients.binding_hash == clients.binding_hash


def test_every_role_factory_rejects_wrong_process_before_key_or_state_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    logical_os.read_paths.clear()
    with logical_os.as_identity(runtime.current_claim.identity):
        with pytest.raises(SourceBrokerV2AuthorityCompositionError, match="process identity"):
            compose_production_source_broker_v2_source_quota_service(runtime)
        with pytest.raises(SourceBrokerV2AuthorityCompositionError, match="process identity"):
            compose_production_source_broker_v2_root_service(
                runtime,
                role=SourceBrokerV2RootRole.CURRENT_CLAIM,
            )
    assert logical_os.read_paths == []

    with logical_os.as_identity(runtime.source_daemon.identity):
        signer = compose_production_source_broker_v2_source_signer(runtime)
    assert signer.authority_id == runtime.source_authority_id
    runtime.source_daemon.public_key_path.write_text("changed", encoding="utf-8")
    runtime.source_daemon.public_key_path.chmod(0o600)
    with (
        logical_os.as_identity(runtime.source_daemon.identity),
        pytest.raises(SourceBrokerV2AuthorityCompositionError, match="source authority key"),
    ):
        compose_production_source_broker_v2_source_signer(runtime)


def test_signed_quota_receipt_is_replayed_after_response_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    now = datetime(2026, 8, 9, 1, 30, tzinfo=UTC)
    SourceQuotaStore(runtime.source_quota.state_path).declare_window(
        source="tushare",
        window_id="minute-1",
        starts_at=now,
        resets_at=now + timedelta(minutes=1),
        total_units=10,
    )
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
        binding = SourceQuotaParentBindingV2(
            parent_id="parent-response-loss",
            source="tushare",
            owner="scheduler",
            claim_binding_hash="a" * 64,
            claim_generation=1,
            scheduler_fencing_token=1,
        )
        service = authorities[1][0]
        service.drop_next_response_after_effect_for_test()  # type: ignore[union-attr]
        with pytest.raises(
            authority_service_module.SourceBrokerV2AuthorityServiceError,
            match="unavailable|untrusted|ended early",
        ):
            clients.source_quota.reserve_parent(
                operation_id="quota-reserve-response-loss",
                binding=binding,
                total_cost=3,
                now=now,
                expires_at=now + timedelta(seconds=30),
            )
        recovered = clients.source_quota.reserve_parent(
            operation_id="quota-reserve-response-loss",
            binding=binding,
            total_cost=3,
            now=now,
            expires_at=now + timedelta(seconds=30),
        )
        replayed = clients.source_quota.reserve_parent(
            operation_id="quota-reserve-response-loss",
            binding=binding,
            total_cost=3,
            now=now,
            expires_at=now + timedelta(seconds=30),
        )
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)

    assert recovered == replayed
    assert recovered.receipt.operation_id == "quota-reserve-response-loss"
    assert recovered.receipt.signature


def test_production_saga_factory_binds_one_composed_unix_client_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The saga can only be created from the single preflighted scheduler graph."""

    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            logical_os.read_paths.clear()
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / "production-saga.sqlite3",
                saga_id="production-saga",
                runtime=runtime,
                scheduler_clients=clients,
            )

        assert saga.production_binding_hash == clients.binding_hash
        assert saga._current_claim_authority is clients.current_claim
        assert saga._quota_adapter._client is clients.source_quota
        assert saga._lineage_authority is clients.replay_lineage
        assert saga._transport is clients.source_client
        assert saga._source_authority_keyring is clients.source_authority_keyring
        assert logical_os.read_paths == []

        with pytest.raises(TypeError, match="requires for_production"):
            SourceBrokerV2Saga(
                tmp_path / "legacy-production-saga.sqlite3",
                saga_id="legacy-production-saga",
                current_claim_authority=clients.current_claim,
                quota_adapter=object(),
                transport=clients.source_client,
                lineage_authority=clients.replay_lineage,
                source_authority_keyring=clients.source_authority_keyring,
                executor_lease_seconds=runtime.executor_lease_seconds,
                source_takeover_grace_seconds=runtime.source_takeover_grace_seconds,
            )

        copied_clients = replace(clients)
        copied_saga = SourceBrokerV2Saga.for_production(
            tmp_path / "copied-graph.sqlite3",
            saga_id="copied-graph",
            runtime=runtime,
            scheduler_clients=copied_clients,
        )
        assert copied_saga._transport is clients.source_client
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_production_authorization_exposes_no_submitable_attestation_or_registry() -> None:
    assert not hasattr(runner_module, "_ISSUED_ATTESTATIONS")
    assert not hasattr(runner_module, "_ATTESTATION_TOKEN")
    assert not hasattr(runner_module, "SourceBrokerV2ProductionAttestation")
    assert "attestation" not in inspect.signature(SourceBrokerV2Saga.for_production).parameters
    assert (
        "attestation"
        not in inspect.signature(
            runner_module.SourceBrokerV2StrictNativeEvidenceVerifier.for_production
        ).parameters
    )
    assert (
        "attestation"
        not in inspect.signature(
            runner_module.SourceBrokerV2ProviderRegistration.for_production
        ).parameters
    )


def test_production_saga_quota_bridge_returns_a_unix_authority_receipt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production quota receipts originate from the composed Unix authority client."""

    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    now = datetime(2026, 8, 9, 1, 30, tzinfo=UTC)
    SourceQuotaStore(runtime.source_quota.state_path).declare_window(
        source="tushare",
        window_id="bridge-minute-1",
        starts_at=now,
        resets_at=now + timedelta(minutes=1),
        total_units=10,
    )
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / "quota-bridge-saga.sqlite3",
                saga_id="quota-bridge-saga",
                runtime=runtime,
                scheduler_clients=clients,
            )
            receipt = saga._quota_adapter.reserve_parent(
                operation_id="quota-bridge-reserve",
                binding=SourceQuotaParentBindingV2(
                    parent_id="quota-bridge-parent",
                    source="tushare",
                    owner="scheduler",
                    claim_binding_hash="a" * 64,
                    claim_generation=1,
                    scheduler_fencing_token=1,
                ),
                total_cost=3,
                now=now,
                expires_at=now + timedelta(seconds=30),
            )

        assert receipt.adapter_id.startswith("source-broker-v2-unix-quota-")
        assert receipt.authority_result.receipt.authority_id == runtime.source_quota.authority_id
        assert receipt.authority_result.receipt.signature
        assert not hasattr(saga._quota_adapter, "_authority")
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_production_saga_persists_and_revalidates_its_unix_graph_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A copied, replaced, or durably rebound graph cannot resume a production Saga."""

    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    path = tmp_path / "bound-production-saga.sqlite3"
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                path,
                saga_id="bound-production-saga",
                runtime=runtime,
                scheduler_clients=clients,
            )

        with sqlite3.connect(path) as connection:
            row = connection.execute(
                "SELECT binding_hash FROM source_broker_v2_production_binding WHERE saga_id = ?",
                ("bound-production-saga",),
            ).fetchone()
        assert row == (clients.binding_hash,)

        graph = saga._production_graph
        assert graph is not None
        with pytest.raises(TypeError, match="cannot be copied"):
            copy.copy(graph)

        original_source_client = clients.source_client
        object.__setattr__(clients, "source_client", object())
        with pytest.raises(TypeError, match="production|scheduler"):
            saga._require_live_production_graph()
        object.__setattr__(clients, "source_client", original_source_client)

        with sqlite3.connect(path) as connection:
            connection.execute(
                "UPDATE source_broker_v2_production_binding SET binding_hash = ? WHERE saga_id = ?",
                ("0" * 64, "bound-production-saga"),
            )
            connection.commit()
        with pytest.raises(Exception, match="production graph binding"):
            SourceBrokerV2Saga.for_production(
                path,
                saga_id="bound-production-saga",
                runtime=runtime,
                scheduler_clients=clients,
            )
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_production_saga_rejects_rebound_local_authority_references(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No production effect may fall back to a replaced in-process authority."""

    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / "rebound-production-saga.sqlite3",
                saga_id="rebound-production-saga",
                runtime=runtime,
                scheduler_clients=clients,
            )

        object.__setattr__(saga, "_current_claim_authority", object())
        with pytest.raises(TypeError, match="production saga graph was replaced"):
            saga._require_live_production_graph()
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


@pytest.mark.parametrize("surface", ("scheduler", "graph_and_saga"))
@pytest.mark.parametrize(
    "component",
    ("current_claim", "quota", "lineage", "source", "source_keyring", "all"),
)
def test_production_saga_rejects_each_synchronized_component_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
    component: str,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / f"rebound-{surface}-{component}.sqlite3",
                saga_id=f"rebound-{surface}-{component}",
                runtime=runtime,
                scheduler_clients=clients,
            )

        graph = saga._production_graph
        assert graph is not None
        components = (
            ("current_claim", "current_claim", "current_claim", "_current_claim_authority"),
            ("quota", "source_quota", "quota", "_quota_adapter"),
            ("lineage", "replay_lineage", "lineage", "_lineage_authority"),
            ("source", "source_client", "source", "_transport"),
            (
                "source_keyring",
                "source_authority_keyring",
                "source_keyring",
                "_source_authority_keyring",
            ),
        )
        selected = (
            components
            if component == "all"
            else tuple(entry for entry in components if entry[0] == component)
        )
        assert selected
        for _label, client_field, graph_field, saga_field in selected:
            replacement = object()
            if surface == "scheduler":
                object.__setattr__(clients, client_field, replacement)
                continue
            object.__setattr__(graph, graph_field, replacement)
            if saga_field == "_quota_adapter":
                object.__setattr__(saga._quota_adapter, "_client", replacement)
            else:
                object.__setattr__(saga, saga_field, replacement)

        with pytest.raises(TypeError, match="production|scheduler"):
            saga._require_live_production_graph()
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_legacy_attestation_objects_cannot_be_submitted_to_production_factories() -> None:
    forged = object.__new__(object)
    for factory in (
        SourceBrokerV2Saga.for_production,
        runner_module.SourceBrokerV2StrictNativeEvidenceVerifier.for_production,
        runner_module.SourceBrokerV2ProviderRegistration.for_production,
    ):
        with pytest.raises(TypeError, match="unexpected keyword argument 'attestation'"):
            inspect.signature(factory).bind_partial(attestation=forged)


@pytest.mark.parametrize(
    ("field", "member"),
    (
        ("source_authority", "authority_id"),
        ("source_authority", "fence_hash"),
        ("claim_authority", "fence_hash"),
        ("quota_authority", "fence_hash"),
        ("lineage_authority", "fence_hash"),
        ("external_root_hash", None),
    ),
)
def test_production_graph_rederives_runtime_authority_evidence_on_every_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    member: str | None,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / f"evidence-{field}-{member}.sqlite3",
                saga_id=f"evidence-{field}-{member}",
                runtime=runtime,
                scheduler_clients=clients,
            )

        graph = saga._production_graph
        assert graph is not None
        evidence = graph._evidence
        if member is None:
            replacement = "0" * 64
        else:
            current = getattr(evidence, field)
            replacement = current.model_copy(update={member: "0" * 64})
        object.__setattr__(evidence, field, replacement)

        with pytest.raises(Exception, match="authority|root|evidence|components"):
            saga._require_live_production_graph()
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


@pytest.mark.parametrize("replacement", ("runtime", "clients"))
def test_production_graph_rejects_equivalent_root_object_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / "runtime-replacement.sqlite3",
                saga_id="runtime-replacement",
                runtime=runtime,
                scheduler_clients=clients,
            )

        graph = saga._production_graph
        assert graph is not None
        if replacement == "runtime":
            copied_runtime = runtime.model_copy(deep=True)
            assert copied_runtime == runtime
            assert copied_runtime is not runtime
            object.__setattr__(graph, "_runtime", copied_runtime)
        else:
            copied_clients = replace(clients)
            assert copied_clients == clients
            assert copied_clients is not clients
            object.__setattr__(graph, "_clients", copied_clients)
        with pytest.raises(TypeError, match="runtime|clients|production"):
            saga._require_live_production_graph()
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


@pytest.mark.parametrize(
    "replacement",
    ("runtime_type", "clients_type", "clients_binding", "clients_component"),
)
def test_production_factory_rejects_wrong_runtime_or_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            candidate_runtime: object = runtime
            candidate_clients: object = clients
            if replacement == "runtime_type":
                candidate_runtime = object()
            elif replacement == "clients_type":
                candidate_clients = object()
            elif replacement == "clients_binding":
                candidate_clients = replace(clients, binding_hash="0" * 64)
            else:
                candidate_clients = replace(clients, current_claim=object())  # type: ignore[arg-type]

            with pytest.raises((TypeError, runner_module.SourceBrokerV2RunnerError)):
                SourceBrokerV2Saga.for_production(
                    tmp_path / f"wrong-{replacement}.sqlite3",
                    saga_id=f"wrong-{replacement}",
                    runtime=candidate_runtime,
                    scheduler_clients=candidate_clients,
                )
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


@pytest.mark.parametrize(
    "client_field",
    ("current_claim", "source_quota", "replay_lineage"),
)
def test_production_graph_fails_closed_when_authority_preflight_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    client_field: str,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / f"preflight-{client_field}.sqlite3",
                saga_id=f"preflight-{client_field}",
                runtime=runtime,
                scheduler_clients=clients,
            )

        client = getattr(clients, client_field)

        def reject_preflight(
            _self: object,
            *,
            deadline: float | None = None,
        ) -> object:
            del deadline
            raise runner_module.SourceBrokerV2RunnerError("injected preflight failure")

        monkeypatch.setattr(type(client), "preflight", reject_preflight)
        with pytest.raises(runner_module.SourceBrokerV2RunnerError, match="preflight"):
            saga._require_live_production_graph()
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_production_graph_rejects_source_endpoint_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / "source-endpoint-replacement.sqlite3",
                saga_id="source-endpoint-replacement",
                runtime=runtime,
                scheduler_clients=clients,
            )

        endpoint = clients.source_client._endpoint
        object.__setattr__(
            clients.source_client,
            "_endpoint",
            endpoint.__class__(
                path=tmp_path / "replacement.sock",
                owner_uid=endpoint.owner_uid,
                group_gid=endpoint.group_gid,
                mode=endpoint.mode,
            ),
        )
        with pytest.raises(Exception, match="source endpoint|scheduler|production"):
            saga._require_live_production_graph()
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_production_graph_rejects_internal_evidence_and_saga_double_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            saga = SourceBrokerV2Saga.for_production(
                tmp_path / "evidence-saga-double-replacement.sqlite3",
                saga_id="evidence-saga-double-replacement",
                runtime=runtime,
                scheduler_clients=clients,
            )

        graph = saga._production_graph
        assert graph is not None
        evidence = graph._evidence
        object.__setattr__(
            evidence,
            "source_authority",
            evidence.source_authority.model_copy(update={"authority_id": "forged"}),
        )
        replacement = object()
        object.__setattr__(graph, "source", replacement)
        object.__setattr__(saga, "_transport", replacement)

        with pytest.raises(Exception, match="authority|evidence|production"):
            saga._require_live_production_graph()
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


@pytest.mark.parametrize("replacement", ("runtime", "clients"))
def test_production_runner_graph_rejects_equivalent_root_object_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            verifier = runner_module.SourceBrokerV2StrictNativeEvidenceVerifier.for_production(
                runtime=runtime,
                scheduler_clients=clients,
            )

        graph = verifier._production_graph
        assert graph is not None
        if replacement == "runtime":
            object.__setattr__(graph, "runtime", runtime.model_copy(deep=True))
        else:
            object.__setattr__(graph, "scheduler_clients", replace(clients))

        with pytest.raises(TypeError, match="runtime|client|production"):
            graph.require_live()
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


@pytest.mark.parametrize(
    "replacement",
    ("registration_binding", "registry_entry", "binding_double", "forged_verifier"),
)
def test_production_registry_rejects_mutation_before_transport_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement: str,
) -> None:
    class WrongTransport:
        def __init__(self) -> None:
            self.effects = 0

        def _effect(self, _payload: bytes, *, deadline: float | None = None) -> bytes:
            del deadline
            self.effects += 1
            return b"{}"

        claim_once = _effect
        replay = _effect
        dispatch = _effect
        finalize = _effect

    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    wrong_transport = WrongTransport()
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            registration = runner_module.SourceBrokerV2ProviderRegistration.for_production(
                runtime=runtime,
                scheduler_clients=clients,
            )
            registry = runner_module.SourceBrokerV2StaticProviderRegistry.for_production(
                runtime=runtime,
                scheduler_clients=clients,
                registrations={"daily-bars": registration},
            )
            runner = _stage_bound_runner(
                db_path=tmp_path / f"registry-mutation-{replacement}.sqlite3",
                registry=registry,
                config=runner_module.SourceBrokerV2JobRunnerConfig(
                    owner_id=f"registry-mutation-{replacement}",
                    lease_seconds=3.1,
                    total_deadline_seconds=3.0,
                    takeover_grace_seconds=0.05,
                ),
            )

        original_binding = registration._binding
        assert original_binding is not None
        forged_verifier = object.__new__(runner_module.SourceBrokerV2StrictNativeEvidenceVerifier)
        forged_verifier._profile = runner_module.SourceBrokerV2RegistryProfile.PRODUCTION
        forged_binding = runner_module.SourceBrokerV2ProviderBinding(
            transport=wrong_transport,
            verifier=forged_verifier,
        )
        if replacement == "registration_binding":
            registration._binding = forged_binding
        elif replacement == "registry_entry":
            forged_registration = object.__new__(runner_module.SourceBrokerV2ProviderRegistration)
            forged_registration.profile = runner_module.SourceBrokerV2RegistryProfile.PRODUCTION
            forged_registration.credential_policy = runner_module.SourceBrokerV2CredentialPolicy()
            forged_registration._factory = None
            forged_registration._binding = forged_binding
            registry._registrations["daily-bars"] = forged_registration
        elif replacement == "binding_double":
            object.__setattr__(original_binding, "transport", wrong_transport)
            object.__setattr__(original_binding, "verifier", forged_verifier)
        else:
            object.__setattr__(original_binding, "verifier", forged_verifier)

        with pytest.raises(Exception, match="production|binding|registry|verifier"):
            runner._transport_call(
                source_id="daily-bars",
                binding=original_binding,
                operation="replay",
                payload=b"{}",
                deadline=time.monotonic() + 1,
            )
        assert wrong_transport.effects == 0
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_production_registry_rejects_object_new_registration_and_verifier(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class WrongTransport:
        effects = 0

        def _effect(self, _payload: bytes, *, deadline: float | None = None) -> bytes:
            del deadline
            self.effects += 1
            return b"{}"

        claim_once = _effect
        replay = _effect
        dispatch = _effect
        finalize = _effect

    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    wrong_transport = WrongTransport()
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            forged_verifier = object.__new__(
                runner_module.SourceBrokerV2StrictNativeEvidenceVerifier
            )
            forged_verifier._profile = runner_module.SourceBrokerV2RegistryProfile.PRODUCTION
            forged_registration = object.__new__(runner_module.SourceBrokerV2ProviderRegistration)
            forged_registration.profile = runner_module.SourceBrokerV2RegistryProfile.PRODUCTION
            forged_registration.credential_policy = runner_module.SourceBrokerV2CredentialPolicy()
            forged_registration._factory = None
            forged_registration._binding = runner_module.SourceBrokerV2ProviderBinding(
                transport=wrong_transport,
                verifier=forged_verifier,
            )

            with pytest.raises(Exception, match="production|registration|graph"):
                runner_module.SourceBrokerV2StaticProviderRegistry.for_production(
                    runtime=runtime,
                    scheduler_clients=clients,
                    registrations={"daily-bars": forged_registration},
                )
        assert wrong_transport.effects == 0
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_provider_registry_profiles_cannot_cross_the_scheduler_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            production = runner_module.SourceBrokerV2ProviderRegistration.for_production(
                runtime=runtime,
                scheduler_clients=clients,
            )
            production_binding = production.open()
            nonproduction = runner_module.SourceBrokerV2ProviderRegistration.for_nonproduction_test(
                factory=lambda _credentials: production_binding,
                credential_policy=runner_module.SourceBrokerV2CredentialPolicy(),
            )

            with pytest.raises(Exception, match="nonproduction|production|profile"):
                runner_module.SourceBrokerV2StaticProviderRegistry.for_production(
                    runtime=runtime,
                    scheduler_clients=clients,
                    registrations={"daily-bars": nonproduction},
                )
            with pytest.raises(TypeError, match="nonproduction|production|profile"):
                runner_module.SourceBrokerV2StaticProviderRegistry.for_nonproduction_test(
                    {"daily-bars": production}
                )
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def _install_transport_request_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[dict[str, int], list[float | None], dict[str, int]]:
    counts = {"current_claim": 0, "source_quota": 0, "replay_lineage": 0, "endpoint": 0}
    deadlines: list[float | None] = []
    effects = {operation: 0 for operation in ("claim_once", "replay", "dispatch", "finalize")}
    for label, client_type in (
        ("current_claim", authority_service_module.SourceBrokerV2CurrentClaimUnixClient),
        ("source_quota", authority_service_module.SourceBrokerV2SourceQuotaUnixClient),
        ("replay_lineage", authority_service_module.SourceBrokerV2ReplayLineageUnixClient),
    ):
        original = client_type.preflight

        def counted_preflight(
            self: object,
            *,
            deadline: float | None = None,
            _label: str = label,
            _original: object = original,
        ) -> object:
            counts[_label] += 1
            deadlines.append(deadline)
            if deadline is None:
                return _original(self)  # type: ignore[operator]
            return _original(self, deadline=deadline)  # type: ignore[operator]

        monkeypatch.setattr(client_type, "preflight", counted_preflight)

    original_endpoint_validation = runner_module.validate_socket_endpoint

    def counted_endpoint_validation(policy: object) -> object:
        counts["endpoint"] += 1
        return original_endpoint_validation(policy)  # type: ignore[arg-type]

    monkeypatch.setattr(
        runner_module,
        "validate_socket_endpoint",
        counted_endpoint_validation,
    )

    for operation in effects:

        def transport_effect(
            _self: object,
            _payload: bytes,
            *,
            deadline: float | None = None,
            _operation: str = operation,
        ) -> bytes:
            effects[_operation] += 1
            deadlines.append(deadline)
            return b"{}"

        monkeypatch.setattr(
            runner_module.SourceBrokerV2UnixClient,
            operation,
            transport_effect,
        )
    return counts, deadlines, effects


def _production_registry_runner(
    *,
    tmp_path: Path,
    runtime: SourceBrokerV2AuthorityRuntime,
    clients: authority_module.SourceBrokerV2SchedulerClients,
    registration_count: int,
    suffix: str,
) -> tuple[
    runner_module.SourceBrokerV2ProviderRegistration,
    runner_module.SourceBrokerV2StaticProviderRegistry,
    runner_module.SourceBrokerV2JobRunner,
]:
    registration = runner_module.SourceBrokerV2ProviderRegistration.for_production(
        runtime=runtime,
        scheduler_clients=clients,
    )
    registrations = {f"source-{index}": registration for index in range(registration_count)}
    registry = runner_module.SourceBrokerV2StaticProviderRegistry.for_production(
        runtime=runtime,
        scheduler_clients=clients,
        registrations=registrations,
    )
    job_runner = _stage_bound_runner(
        db_path=tmp_path / f"request-proof-{suffix}.sqlite3",
        registry=registry,
        config=runner_module.SourceBrokerV2JobRunnerConfig(
            owner_id=f"request-proof-{suffix}",
            lease_seconds=3.1,
            total_deadline_seconds=3.0,
            takeover_grace_seconds=0.05,
        ),
    )
    return registration, registry, job_runner


def test_each_production_transport_request_uses_one_deadline_bound_live_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            registration, _registry, job_runner = _production_registry_runner(
                tmp_path=tmp_path,
                runtime=runtime,
                clients=clients,
                registration_count=1,
                suffix="four-operations",
            )
        binding = registration._binding
        assert binding is not None
        counts, deadlines, effects = _install_transport_request_probe(monkeypatch)

        for operation in ("claim_once", "replay", "dispatch", "finalize"):
            before = counts.copy()
            observed_deadlines = len(deadlines)
            request_deadline = time.monotonic() + 1.0
            assert (
                job_runner._transport_call(
                    source_id="source-0",
                    binding=binding,
                    operation=operation,
                    payload=b"{}",
                    deadline=request_deadline,
                )
                == b"{}"
            )
            assert {name: counts[name] - before[name] for name in counts} == {
                "current_claim": 1,
                "source_quota": 1,
                "replay_lineage": 1,
                "endpoint": 1,
            }
            assert effects[operation] == 1
            assert deadlines[observed_deadlines:] == [
                request_deadline,
                request_deadline,
                request_deadline,
                request_deadline,
            ]
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_production_transport_live_proof_cost_is_independent_of_registry_size(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            built = {
                count: _production_registry_runner(
                    tmp_path=tmp_path,
                    runtime=runtime,
                    clients=clients,
                    registration_count=count,
                    suffix=f"size-{count}",
                )
                for count in (1, 10, 100)
            }
        counts, _deadlines, effects = _install_transport_request_probe(monkeypatch)

        for registration_count, (registration, _registry, job_runner) in built.items():
            binding = registration._binding
            assert binding is not None
            before = counts.copy()
            job_runner._transport_call(
                source_id=f"source-{registration_count - 1}",
                binding=binding,
                operation="replay",
                payload=b"{}",
                deadline=time.monotonic() + 1.0,
            )
            assert {name: counts[name] - before[name] for name in counts} == {
                "current_claim": 1,
                "source_quota": 1,
                "replay_lineage": 1,
                "endpoint": 1,
            }
        assert effects["replay"] == 3
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_production_transport_live_proof_is_never_cached_across_requests(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            registration, _registry, job_runner = _production_registry_runner(
                tmp_path=tmp_path,
                runtime=runtime,
                clients=clients,
                registration_count=1,
                suffix="no-cache",
            )
        binding = registration._binding
        assert binding is not None
        counts, _deadlines, effects = _install_transport_request_probe(monkeypatch)

        for _request in range(2):
            job_runner._transport_call(
                source_id="source-0",
                binding=binding,
                operation="replay",
                payload=b"{}",
                deadline=time.monotonic() + 1.0,
            )
        assert counts == {
            "current_claim": 2,
            "source_quota": 2,
            "replay_lineage": 2,
            "endpoint": 2,
        }
        assert effects["replay"] == 2
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_slow_production_preflight_consumes_shared_deadline_before_transport_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    effects = 0
    observed_deadlines: list[float | None] = []
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            registration, _registry, job_runner = _production_registry_runner(
                tmp_path=tmp_path,
                runtime=runtime,
                clients=clients,
                registration_count=1,
                suffix="slow-preflight",
            )
        binding = registration._binding
        assert binding is not None

        def slow_preflight(
            _self: object,
            *,
            deadline: float | None = None,
        ) -> object:
            observed_deadlines.append(deadline)
            if deadline is None:
                raise AssertionError("runner absolute deadline was not propagated")
            while time.monotonic() < deadline:
                time.sleep(0.001)
            raise authority_service_module.SourceBrokerV2AuthorityServiceError(
                "authority request deadline expired"
            )

        def transport_effect(
            _self: object,
            _payload: bytes,
            *,
            deadline: float | None = None,
        ) -> bytes:
            del deadline
            nonlocal effects
            effects += 1
            return b"{}"

        monkeypatch.setattr(
            authority_service_module.SourceBrokerV2CurrentClaimUnixClient,
            "preflight",
            slow_preflight,
        )
        monkeypatch.setattr(
            runner_module.SourceBrokerV2UnixClient,
            "replay",
            transport_effect,
        )
        request_deadline = time.monotonic() + 0.04
        started = time.monotonic()
        with pytest.raises(Exception, match="deadline|absolute"):
            job_runner._transport_call(
                source_id="source-0",
                binding=binding,
                operation="replay",
                payload=b"{}",
                deadline=request_deadline,
            )
        assert time.monotonic() - started < 0.2
        assert observed_deadlines == [request_deadline]
        assert effects == 0
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_runner_rejects_synchronized_registry_graph_entry_and_binding_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    roots = _start_root_services(runtime, logical_os)
    authorities: list[tuple[object, threading.Thread]] = []
    effects = 0
    try:
        authorities = _start_authority_services(runtime, logical_os)
        with logical_os.as_identity(runtime.scheduler_client.identity):
            clients = compose_production_source_broker_v2_scheduler_clients(runtime)
            _registration, registry, job_runner = _production_registry_runner(
                tmp_path=tmp_path,
                runtime=runtime,
                clients=clients,
                registration_count=1,
                suffix="synchronized-replacement",
            )
            replacement = runner_module.SourceBrokerV2ProviderRegistration.for_production(
                runtime=runtime,
                scheduler_clients=clients,
            )
        replacement_binding = replacement._binding
        assert replacement_binding is not None
        replacement_entry = (
            "source-0",
            replacement,
            replacement_binding,
            replacement_binding.transport,
            replacement_binding.verifier,
        )
        registry._production_graph = replacement._production_graph
        registry._registrations = {"source-0": replacement}
        registry._original_entries = (replacement_entry,)
        registry._original_entry_by_source = MappingProxyType({"source-0": replacement_entry})

        def transport_effect(
            _self: object,
            _payload: bytes,
            *,
            deadline: float | None = None,
        ) -> bytes:
            del deadline
            nonlocal effects
            effects += 1
            return b"{}"

        monkeypatch.setattr(
            runner_module.SourceBrokerV2UnixClient,
            "replay",
            transport_effect,
        )
        with pytest.raises(Exception, match="production|registry|graph|binding"):
            job_runner._transport_call(
                source_id="source-0",
                binding=replacement_binding,
                operation="replay",
                payload=b"{}",
                deadline=time.monotonic() + 1.0,
            )
        assert effects == 0
    finally:
        _stop_authority_services(authorities)
        _stop_root_services(roots)


def test_composition_rejects_a_same_owner_socket_replacement_without_root_key(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, logical_os = _runtime(tmp_path, monkeypatch)
    started = _start_root_services(runtime, logical_os)
    root = runtime.root(SourceBrokerV2RootRole.CURRENT_CLAIM)
    first_service, first_thread, first_stop = started[0]
    first_stop.set()
    first_service.wake()  # type: ignore[union-attr]
    first_thread.join(timeout=5)
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as replacement:
            replacement.bind(str(root.socket_path))
            root.socket_path.chmod(0o660)
            replacement.listen(1)
            dropped = threading.Event()

            def drop_request() -> None:
                connection, _address = replacement.accept()
                with connection:
                    connection.recv(4096)
                dropped.set()

            dropper = threading.Thread(target=drop_request)
            dropper.start()
            with (
                logical_os.as_identity(runtime.current_claim.identity),
                pytest.raises(
                    SourceBrokerV2AuthorityCompositionError,
                    match="unavailable|untrusted",
                ),
            ):
                compose_production_source_broker_v2_current_claim_service(runtime)
            assert dropped.wait(timeout=2)
            dropper.join(timeout=2)
    finally:
        _stop_root_services(started[1:])
        root.socket_path.unlink(missing_ok=True)
