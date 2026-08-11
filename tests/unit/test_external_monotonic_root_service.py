from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import tempfile
import threading
from pathlib import Path

import pytest

from rquant.external_monotonic_root import (
    ExternalMonotonicRootRequest,
    UnixSocketExternalMonotonicRootClient,
    UnixSocketExternalMonotonicRootManifest,
)
from rquant.external_monotonic_root_service import (
    ClosedExternalMonotonicRootVerifier,
    ExternalMonotonicRootUnixService,
    ExternalRootServiceConfiguration,
    OpenSslExternalMonotonicRootSigner,
    PersistentExternalMonotonicRootBackend,
    probe_external_monotonic_root_service,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import canonical_json_bytes

ROLE = "resource_journal_monotonic_root"
AUTHORITY = "external-resource-root"
STORE = "external-resource-root-store"
SUBJECT = "resource-authority"
NAMESPACE = "rquant-resource-journal-anti-rollback-root-receipt/v1"
PROBE_NAMESPACE = "rquant-external-monotonic-root-service-probe/v1"


def _private_socket_parent() -> Path:
    parent = Path(__file__).resolve().parents[2] / ".s"
    parent.mkdir(parents=True, exist_ok=True)
    parent.chmod(0o700)
    return parent


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required")
    return executable


def _signing_pair(
    root: Path,
    *,
    issuer: str = "resource-root-issuer",
    key_id: str = "resource-root-key",
    key_purpose: str = "resource-journal-high-water",
    namespaces: frozenset[str] = frozenset({NAMESPACE, PROBE_NAMESPACE}),
) -> tuple[OpenSslExternalMonotonicRootSigner, ClosedExternalMonotonicRootVerifier]:
    root.mkdir(parents=True, exist_ok=True)
    private = root / "root.private.pem"
    public = root / "root.public.pem"
    subprocess.run(
        (_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private)),
        check=True,
        capture_output=True,
    )
    subprocess.run(
        (_openssl(), "pkey", "-in", str(private), "-pubout", "-out", str(public)),
        check=True,
        capture_output=True,
    )
    private.chmod(0o600)
    public.chmod(0o600)
    signer = OpenSslExternalMonotonicRootSigner(
        private_key_path=private,
        public_key_path=public,
        issuer=issuer,
        key_id=key_id,
        key_purpose=key_purpose,
        allowed_namespaces=namespaces,
    )
    verifier = ClosedExternalMonotonicRootVerifier(
        public_key_path=public,
        issuer=signer.issuer,
        key_id=signer.key_id,
        key_purpose=signer.key_purpose,
    )
    return signer, verifier


def _request(
    *,
    kind: str,
    operation_id: str | None = None,
    previous: str | None = None,
    sequence: int | None = None,
    challenge_nonce: str = "9" * 64,
) -> ExternalMonotonicRootRequest:
    values: dict[str, object] = {
        "kind": kind,
        "role": ROLE,
        "root_authority_id": AUTHORITY,
        "root_store_id": STORE,
        "subject_authority_id": SUBJECT,
        "challenge_nonce": challenge_nonce,
    }
    if kind != "current":
        checkpoint = {
            "contract": "test-monotonic-checkpoint/v1",
            "sequence": sequence,
        }
        values.update(
            operation_id=operation_id,
            previous_checkpoint_hash=previous,
            checkpoint_contract=checkpoint["contract"],
            checkpoint_hash=canonical_sha256(checkpoint),
            checkpoint_json=canonical_json_bytes(checkpoint).decode("utf-8"),
        )
    return ExternalMonotonicRootRequest.close(**values)


def test_persistent_backend_recovers_cas_and_idempotency_after_restart(tmp_path: Path) -> None:
    path = tmp_path / "root.sqlite3"
    backend = PersistentExternalMonotonicRootBackend(
        path,
        role=ROLE,
        authority_id=AUTHORITY,
        store_id=STORE,
    )
    pin = _request(kind="pin", operation_id="1" * 64, previous="0" * 64, sequence=0)
    pinned = backend.apply(pin)
    assert pinned is not None

    reopened = PersistentExternalMonotonicRootBackend(
        path,
        role=ROLE,
        authority_id=AUTHORITY,
        store_id=STORE,
    )
    assert reopened.apply(pin) == pinned
    advance = _request(
        kind="advance",
        operation_id="2" * 64,
        previous=pinned.checkpoint_hash,
        sequence=1,
    )
    advanced = reopened.apply(advance)
    assert advanced is not None
    assert reopened.apply(_request(kind="current")) == advanced
    with pytest.raises(RuntimeError, match="rebound"):
        reopened.apply(
            _request(
                kind="advance",
                operation_id="2" * 64,
                previous=advanced.checkpoint_hash,
                sequence=2,
            )
        )

    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE root_operation SET journal_hash = ? WHERE operation_id = ?",
            ("f" * 64, "1" * 64),
        )
    with pytest.raises(RuntimeError, match="integrity"):
        PersistentExternalMonotonicRootBackend(
            path,
            role=ROLE,
            authority_id=AUTHORITY,
            store_id=STORE,
        )


def test_persistent_backend_rejects_a_writable_state_ancestor(tmp_path: Path) -> None:
    unsafe = tmp_path / "unsafe"
    state = unsafe / "state"
    state.mkdir(parents=True, mode=0o700)
    unsafe.chmod(0o770)

    with pytest.raises(RuntimeError, match="backend directory"):
        PersistentExternalMonotonicRootBackend(
            state / "root.sqlite3",
            role=ROLE,
            authority_id=AUTHORITY,
            store_id=STORE,
        )
    assert not (state / "root.sqlite3").exists()


def test_signing_key_rejects_a_writable_ancestor(tmp_path: Path) -> None:
    unsafe = tmp_path / "unsafe"
    key_root = unsafe / "keys"
    _signer, _verifier = _signing_pair(key_root)
    unsafe.chmod(0o770)

    with pytest.raises(RuntimeError, match="key"):
        OpenSslExternalMonotonicRootSigner(
            private_key_path=key_root / "root.private.pem",
            public_key_path=key_root / "root.public.pem",
            issuer="resource-root-issuer",
            key_id="resource-root-key",
            key_purpose="resource-journal-high-water",
            allowed_namespaces=frozenset({NAMESPACE, PROBE_NAMESPACE}),
        )


def test_external_root_socket_rejects_a_writable_ancestor(tmp_path: Path) -> None:
    signer, _verifier = _signing_pair(tmp_path / "keys")
    short_root = Path(tempfile.mkdtemp(prefix="rqsu-", dir=_private_socket_parent())).resolve()
    unsafe = short_root / "unsafe"
    socket_root = unsafe / "run"
    socket_root.mkdir(parents=True, mode=0o750)
    unsafe.chmod(0o770)
    socket_path = socket_root / "external-root.sock"
    configuration = ExternalRootServiceConfiguration(
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        service_uid=os.getuid(),
        service_gid=os.getgid(),
        allowed_peer_uid=os.getuid(),
        allowed_peer_gid=os.getgid(),
        socket_mode=0o660,
        socket_directory_mode=0o750,
        role=ROLE,
        authority_id=AUTHORITY,
        store_id=STORE,
        rollback_domain_id="external-root-domain",
        transport_manifest_hash="a" * 64,
    )
    service = ExternalMonotonicRootUnixService(
        configuration=configuration,
        backend=PersistentExternalMonotonicRootBackend(
            tmp_path / "root.sqlite3",
            role=ROLE,
            authority_id=AUTHORITY,
            store_id=STORE,
        ),
        handler=_StateHandler(),
        probe_signer=signer,
    )

    try:
        with pytest.raises(RuntimeError, match="socket directory"):
            service.bind()
    finally:
        socket_path.unlink(missing_ok=True)
        socket_root.rmdir()
        unsafe.chmod(0o700)
        unsafe.rmdir()
        short_root.rmdir()


class _StateHandler:
    def response_json(
        self,
        request: ExternalMonotonicRootRequest,
        state: object | None,
    ) -> str | None:
        if state is None:
            return None
        return canonical_json_bytes(
            {
                "checkpoint_hash": state.checkpoint_hash,
                "operation_id": state.operation_id,
                "request_hash": request.request_hash,
            }
        ).decode("utf-8")


def test_unix_daemon_probe_response_loss_and_restart_round_trip(tmp_path: Path) -> None:
    signer, verifier = _signing_pair(tmp_path)
    socket_root = Path(tempfile.mkdtemp(prefix="rqer-", dir=_private_socket_parent())).resolve()
    os.chown(socket_root, os.getuid(), os.getgid())
    socket_root.chmod(0o750)
    socket_path = socket_root / "external-root.sock"
    manifest_seed = UnixSocketExternalMonotonicRootManifest(
        role=ROLE,
        authority_id=AUTHORITY,
        store_id=STORE,
        rollback_domain_id="external-root-domain",
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        socket_mode=0o660,
        peer_uid=os.getuid(),
        peer_gid=os.getgid(),
        connect_timeout_ms=2_000,
        max_response_bytes=1024 * 1024,
    )
    config = ExternalRootServiceConfiguration(
        socket_path=socket_path,
        socket_uid=os.getuid(),
        socket_gid=os.getgid(),
        service_uid=os.getuid(),
        service_gid=os.getgid(),
        allowed_peer_uid=os.getuid(),
        allowed_peer_gid=os.getgid(),
        socket_mode=0o660,
        socket_directory_mode=0o750,
        role=ROLE,
        authority_id=AUTHORITY,
        store_id=STORE,
        rollback_domain_id="external-root-domain",
        transport_manifest_hash=manifest_seed.manifest_hash,
    )
    backend_path = tmp_path / "external-root.sqlite3"

    def start() -> tuple[ExternalMonotonicRootUnixService, threading.Event, threading.Thread]:
        service = ExternalMonotonicRootUnixService(
            configuration=config,
            backend=PersistentExternalMonotonicRootBackend(
                backend_path,
                role=ROLE,
                authority_id=AUTHORITY,
                store_id=STORE,
            ),
            handler=_StateHandler(),
            probe_signer=signer,
        )
        stop = threading.Event()
        thread = threading.Thread(
            target=service.serve_forever,
            kwargs={"stop": stop},
            daemon=True,
        )
        thread.start()
        assert service.ready.wait(timeout=5)
        return service, stop, thread

    service, stop, thread = start()
    client = UnixSocketExternalMonotonicRootClient(manifest_seed)
    try:
        identity = probe_external_monotonic_root_service(
            manifest_seed,
            verifier=verifier,
            expected_transport_manifest_hash=manifest_seed.manifest_hash,
        )
        assert identity.capabilities == ("current", "pin", "advance")
        pin = _request(kind="pin", operation_id="1" * 64, previous="0" * 64, sequence=0)
        service.drop_next_response_after_effect_for_test()
        with pytest.raises(ConnectionError):
            client.invoke(request_json=canonical_json_bytes(pin.model_dump(mode="json")).decode())
    finally:
        stop.set()
        service.wake()
        thread.join(timeout=5)

    restarted, stop2, thread2 = start()
    try:
        replayed = client.invoke(
            request_json=canonical_json_bytes(
                _request(
                    kind="pin",
                    operation_id="1" * 64,
                    previous="0" * 64,
                    sequence=0,
                    challenge_nonce="8" * 64,
                ).model_dump(mode="json")
            ).decode()
        )
        assert replayed is not None
        assert '"request_hash"' in replayed
        recovered = client.invoke(
            request_json=canonical_json_bytes(
                _request(kind="current").model_dump(mode="json")
            ).decode()
        )
        assert recovered is not None
        assert '"operation_id":"' + "1" * 64 + '"' in recovered
    finally:
        stop2.set()
        restarted.wake()
        thread2.join(timeout=5)
        socket_root.rmdir()
