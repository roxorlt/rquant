from __future__ import annotations

import base64
import hashlib
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from rquant.daily_receipt_socket_authority import (  # noqa: E402
    DAILY_RECEIPT_SOCKET_ENDPOINT,
    DailyReceiptSocketSigningClient,
)
from rquant.runtime_builder_daily_orchestrator import (  # noqa: E402
    _DAILY_RUN_RECEIPT_NAMESPACE,
    _DAILY_STAGE_RECEIPT_NAMESPACE,
)
from rquant.runtime_shadow_validation import (  # noqa: E402
    _valid_ed25519_signature,
    _verify_ed25519_signature,
)
from rquant.strict_json import canonical_json_bytes, strict_json_loads  # noqa: E402


def _openssl() -> str:
    executable = (
        "/opt/homebrew/bin/openssl"
        if Path("/opt/homebrew/bin/openssl").exists()
        else "/usr/bin/openssl"
    )
    if not Path(executable).exists():
        pytest.skip("openssl is required for Daily socket authority tests")
    return executable


def _private_key(root: Path, *, key_id: str) -> tuple[Path, str]:
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    subprocess.run(
        [_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)],
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    public_key = subprocess.run(
        [_openssl(), "pkey", "-in", str(private_key), "-pubout"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return private_key, public_key


def _manifest(root: Path, *, key_id: str = "daily-v1") -> tuple[Path, str, str]:
    private_key, public_key = _private_key(root / "keys", key_id=key_id)
    manifest = root / "daily-receipt-keys.json"
    manifest.write_bytes(
        canonical_json_bytes(
            {
                "schema_version": 2,
                "generation": 1,
                "previous_manifest_hash": "0" * 64,
                "active_key_id": key_id,
                "active_private_key_path": str(private_key),
                "previous_public_keys": {},
            }
        )
    )
    manifest.chmod(0o600)
    return manifest, key_id, public_key


def _private_key_from_manifest(manifest: Path) -> Path:
    payload = strict_json_loads(manifest.read_bytes())
    assert isinstance(payload, dict)
    return Path(str(payload["active_private_key_path"]))


def _rotated_manifest(root: Path) -> tuple[Path, str, str, str, str]:
    _previous_private, previous_public = _private_key(root / "keys", key_id="daily-v1")
    active_private, active_public = _private_key(root / "keys", key_id="daily-v2")
    body = {
        "schema_version": 2,
        "generation": 2,
        "previous_manifest_hash": "a" * 64,
        "active_key_id": "daily-v2",
        "active_private_key_path": str(active_private),
        "previous_public_keys": {"daily-v1": previous_public},
    }
    manifest = root / "daily-receipt-keys.json"
    manifest.write_bytes(canonical_json_bytes(body))
    manifest.chmod(0o600)
    return manifest, "daily-v2", active_public, "daily-v1", previous_public


def _start_server(
    tmp_path: Path,
    *,
    keys_path: Path,
    allowed_uid: int | None = None,
    allowed_gid: int | None = None,
    max_connections: int = 64,
    endpoint: Path | None = None,
) -> tuple[subprocess.Popen[bytes], Path]:
    socket_path = (
        Path("/tmp") / f"rqdrs-{os.getpid()}-{time.time_ns()}.sock"
        if endpoint is None
        else endpoint
    )
    nonce_state_root = tmp_path / "daily-receipt-signer-state"
    script = tmp_path / "serve.py"
    script.parent.mkdir(parents=True, exist_ok=True)
    script.write_text(
        "\n".join(
            [
                "import os",
                "import sys",
                "from pathlib import Path",
                "import rquant.daily_receipt_socket_authority as authority",
                f"allowed_uid = {allowed_uid!r}",
                f"allowed_gid = {allowed_gid!r}",
                "if allowed_uid is None:",
                "    allowed_uid = os.getuid()",
                "if allowed_gid is None:",
                "    allowed_gid = os.getgid()",
                "# Test doubles exist only inside this child process, and only where the",
                "# kernel cannot answer at all.  On Linux the real SO_PEERCRED path runs",
                "# unpatched; production main() never patches anything.",
                "if not authority.daily_receipt_peer_credentials_supported():",
                "    if sys.platform.startswith('linux'):",
                "        raise SystemExit('refusing to fake SO_PEERCRED on Linux')",
                "    authority.daily_receipt_peer_credentials_supported = lambda: True",
                "    authority._peer_credentials = (",
                "        lambda _connection: (os.getuid(), os.getgid())",
                "    )",
                "authority.serve_daily_receipt_socket_authority(",
                f"    socket_path=Path({str(socket_path)!r}),",
                f"    keys_path=Path({str(keys_path)!r}),",
                f"    nonce_state_root=Path({str(nonce_state_root)!r}),",
                "    allowed_uid=allowed_uid,",
                "    allowed_gid=allowed_gid,",
                f"    max_connections={max_connections!r},",
                ")",
            ]
        ),
        encoding="utf-8",
    )
    process = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=Path(__file__).resolve().parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                f"server exited early: {process.returncode}\n{stdout!r}\n{stderr!r}"
            )
        if socket_path.exists():
            return process, socket_path
        time.sleep(0.02)
    process.kill()
    stdout, stderr = process.communicate(timeout=1)
    raise AssertionError(f"server did not start\n{stdout!r}\n{stderr!r}")


def _stop(process: subprocess.Popen[bytes]) -> None:
    process.terminate()
    try:
        process.communicate(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=3)


def _kill(process: subprocess.Popen[bytes]) -> None:
    process.send_signal(signal.SIGKILL)
    process.communicate(timeout=5)


def _await_connectable(process: subprocess.Popen[bytes], endpoint: Path) -> None:
    """A stale socket file from SIGKILL makes existence useless; wait for a real accept."""

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate(timeout=1)
            raise AssertionError(
                f"restarted server exited: {process.returncode}\n{stdout!r}\n{stderr!r}"
            )
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.settimeout(1)
            probe.connect(str(endpoint))
        except OSError:
            time.sleep(0.05)
            continue
        finally:
            probe.close()
        return
    raise AssertionError("restarted server never accepted a connection")


def _short_socket_path(name: str) -> Path:
    return Path("/tmp") / f"rqdrs-{os.getpid()}-{time.time_ns()}-{name}.sock"


def test_socket_authority_signs_allowed_peer_and_rejects_spoofed_peer(
    tmp_path: Path,
) -> None:
    keys_path, key_id, public_key = _manifest(tmp_path)
    process, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=2)
    try:
        client = DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=socket_path,
            active_key_id=key_id,
            active_public_key_pem=public_key,
            timeout_seconds=2,
        )
        payload = b"stage receipt payload"
        signature = client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=payload)
        assert _valid_ed25519_signature(signature)
    finally:
        _stop(process)

    rejected, rejected_socket = _start_server(
        tmp_path / "reject",
        keys_path=keys_path,
        allowed_uid=os.getuid() + 1,
        max_connections=1,
    )
    try:
        client = DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=rejected_socket,
            active_key_id=key_id,
            active_public_key_pem=public_key,
            timeout_seconds=2,
        )
        with pytest.raises(RuntimeError, match="peer|authority|failed"):
            client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"denied")
    finally:
        _stop(rejected)


def test_socket_authority_identity_probe_is_nonce_bound_and_signed(
    tmp_path: Path,
) -> None:
    keys_path, key_id, public_key = _manifest(tmp_path)
    process, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=1)
    try:
        client = DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=socket_path,
            active_key_id=key_id,
            active_public_key_pem=public_key,
            timeout_seconds=2,
        )
        response = client.identity()
        assert response.key_id == key_id
        assert len(response.source_sha256) == 64
        source = (
            Path(__file__).resolve().parents[2]
            / "src/rquant/daily_receipt_socket_authority.py"
        )
        assert response.source_sha256 == hashlib.sha256(source.read_bytes()).hexdigest()
    finally:
        _stop(process)


@pytest.mark.skipif(
    sys.platform != "linux" or not hasattr(socket, "SO_PEERCRED"),
    reason="Linux SO_PEERCRED is unavailable",
)
def test_socket_authority_requires_real_linux_peer_credentials(tmp_path: Path) -> None:
    keys_path, key_id, public_key = _manifest(tmp_path)
    process, socket_path = _start_server(
        tmp_path,
        keys_path=keys_path,
        allowed_uid=os.getuid(),
        allowed_gid=os.getgid(),
        max_connections=1,
    )
    try:
        client = DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=socket_path,
            active_key_id=key_id,
            active_public_key_pem=public_key,
            timeout_seconds=2,
        )
        assert client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"linux-peer")
    finally:
        _stop(process)


@pytest.mark.skipif(
    sys.platform != "linux" or not hasattr(socket, "SO_PEERCRED"),
    reason="Linux SO_PEERCRED is unavailable",
)
@pytest.mark.parametrize("forged", ("uid", "gid"))
def test_socket_authority_rejects_forged_peer_identity_on_real_peer_credentials(
    tmp_path: Path,
    forged: str,
) -> None:
    """The kernel answers SO_PEERCRED, so a peer cannot claim another service identity."""

    keys_path, _key_id, _public_key = _manifest(tmp_path)
    process, socket_path = _start_server(
        tmp_path / forged,
        keys_path=keys_path,
        allowed_uid=os.getuid() + 1 if forged == "uid" else None,
        allowed_gid=os.getgid() + 1 if forged == "gid" else None,
        max_connections=1,
    )
    try:
        response = _raw_socket_request_or_error(
            socket_path,
            {
                "version": 1,
                "operation": "sign",
                "namespace": _DAILY_STAGE_RECEIPT_NAMESPACE,
                "nonce": "9" * 64,
                "payload_sha256": hashlib.sha256(b"forged").hexdigest(),
                "canonical_payload": base64.b64encode(b"forged").decode("ascii"),
            },
        )
        assert isinstance(response, RuntimeError)
        assert "peer credentials are not allowed" in str(response)
    finally:
        _stop(process)


def test_socket_authority_recovers_after_kill_on_the_same_endpoint(tmp_path: Path) -> None:
    """SIGKILL leaves a stale socket file; the restarted authority must rebind the endpoint."""

    keys_path, key_id, public_key = _manifest(tmp_path)
    endpoint = _short_socket_path("kill")
    killed, socket_path = _start_server(
        tmp_path,
        keys_path=keys_path,
        max_connections=8,
        endpoint=endpoint,
    )
    client = DailyReceiptSocketSigningClient.for_test_endpoint(
        socket_path=socket_path,
        active_key_id=key_id,
        active_public_key_pem=public_key,
        timeout_seconds=2,
        max_attempts=2,
    )
    assert client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"before-kill")

    _kill(killed)
    assert killed.returncode != 0
    assert endpoint.exists()  # SIGKILL never runs the cleanup path
    with pytest.raises(RuntimeError, match="failed"):
        client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"while-down")

    restarted, restarted_socket = _start_server(
        tmp_path / "restarted",
        keys_path=keys_path,
        max_connections=8,
        endpoint=endpoint,
    )
    try:
        assert restarted_socket == endpoint
        _await_connectable(restarted, endpoint)
        assert client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"after-kill")
    finally:
        _stop(restarted)
        endpoint.unlink(missing_ok=True)


def test_socket_authority_serves_concurrent_clients_while_one_peer_stalls(
    tmp_path: Path,
) -> None:
    """A peer that connects and then goes silent must not wedge the root authority."""

    keys_path, key_id, public_key = _manifest(tmp_path)
    process, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=12)
    stalled = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        stalled.connect(str(socket_path))
        stalled.sendall(b"\x00\x00")  # half a frame header, then silence

        client = DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=socket_path,
            active_key_id=key_id,
            active_public_key_pem=public_key,
            timeout_seconds=3,
            max_attempts=2,
        )
        payloads = [f"concurrent-{index}".encode("ascii") for index in range(6)]
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=6) as pool:
            signatures = tuple(
                pool.map(
                    lambda payload: client.sign(
                        namespace=_DAILY_STAGE_RECEIPT_NAMESPACE,
                        payload=payload,
                    ),
                    payloads,
                )
            )
        elapsed = time.monotonic() - started

        assert len(signatures) == len(set(signatures))
        assert all(_valid_ed25519_signature(signature) for signature in signatures)
        # The stalled peer holds one worker for the full connection timeout; the others
        # must not have queued behind it.
        assert elapsed < 8
    finally:
        stalled.close()
        _stop(process)


def test_socket_authority_signs_with_the_active_key_only_after_rotation(
    tmp_path: Path,
) -> None:
    """Rotation keeps retired keys for history verification, never for new signatures."""

    keys_path, active_key_id, active_public, previous_key_id, previous_public = (
        _rotated_manifest(tmp_path)
    )
    process, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=1)
    try:
        nonce = "7" * 64
        payload = b"rotated-run-payload"
        response = _raw_socket_request(
            socket_path,
            {
                "version": 1,
                "operation": "sign",
                "namespace": _DAILY_RUN_RECEIPT_NAMESPACE,
                "nonce": nonce,
                "payload_sha256": hashlib.sha256(payload).hexdigest(),
                "canonical_payload": base64.b64encode(payload).decode("ascii"),
            },
        )
        assert response["key_id"] == active_key_id
        assert response["key_id"] != previous_key_id
        assert _verify_ed25519_signature(
            public_key=active_public.encode("utf-8"),
            payload=payload,
            signature=str(response["signature"]),
        )
        assert not _verify_ed25519_signature(
            public_key=previous_public.encode("utf-8"),
            payload=payload,
            signature=str(response["signature"]),
        )
    finally:
        _stop(process)


def test_socket_authority_signs_canonical_payload_after_binding_request_envelope(
    tmp_path: Path,
) -> None:
    keys_path, key_id, public_key = _manifest(tmp_path)
    process, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=1)
    try:
        request = {
            "version": 1,
            "operation": "sign",
            "namespace": _DAILY_STAGE_RECEIPT_NAMESPACE,
            "nonce": "1" * 64,
            "payload_sha256": hashlib.sha256(b"payload").hexdigest(),
            "canonical_payload": base64.b64encode(b"payload").decode("ascii"),
        }
        response = _raw_socket_request(socket_path, request)
        request_envelope = {
            "version": 1,
            "operation": "sign",
            "namespace": _DAILY_STAGE_RECEIPT_NAMESPACE,
            "nonce": "1" * 64,
            "payload_sha256": hashlib.sha256(b"payload").hexdigest(),
            "key_id": key_id,
        }
        assert _verify_ed25519_signature(
            public_key=public_key.encode("utf-8"),
            payload=b"payload",
            signature=str(response["signature"]),
        )
        assert not _verify_ed25519_signature(
            public_key=public_key.encode("utf-8"),
            payload=canonical_json_bytes(request_envelope),
            signature=str(response["signature"]),
        )
    finally:
        _stop(process)


def test_socket_client_retries_after_restart_and_handles_concurrent_requests(
    tmp_path: Path,
) -> None:
    keys_path, key_id, public_key = _manifest(tmp_path)
    first, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=1)
    client = DailyReceiptSocketSigningClient.for_test_endpoint(
        socket_path=socket_path,
        active_key_id=key_id,
        active_public_key_pem=public_key,
        timeout_seconds=2,
        max_attempts=5,
    )
    assert client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"first")
    _stop(first)

    restarted, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=16)
    try:
        client = DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=socket_path,
            active_key_id=key_id,
            active_public_key_pem=public_key,
            timeout_seconds=2,
            max_attempts=5,
        )
        payloads = [f"payload-{index}".encode("ascii") for index in range(12)]
        with ThreadPoolExecutor(max_workers=6) as pool:
            signatures = tuple(
                pool.map(
                    lambda payload: client.sign(
                        namespace=_DAILY_STAGE_RECEIPT_NAMESPACE,
                        payload=payload,
                    ),
                    payloads,
                )
            )
        assert len(signatures) == len(set(signatures))
        assert all(_valid_ed25519_signature(signature) for signature in signatures)
    finally:
        _stop(restarted)


def test_socket_authority_persists_nonce_replay_after_restart(tmp_path: Path) -> None:
    keys_path, _key_id, _public_key = _manifest(tmp_path)
    request = {
        "version": 1,
        "operation": "sign",
        "namespace": _DAILY_STAGE_RECEIPT_NAMESPACE,
        "nonce": "2" * 64,
        "payload_sha256": hashlib.sha256(b"restarted").hexdigest(),
        "canonical_payload": base64.b64encode(b"restarted").decode("ascii"),
    }
    first, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=1)
    try:
        assert _raw_socket_request(socket_path, request)["nonce"] == "2" * 64
    finally:
        _stop(first)

    restarted, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=1)
    try:
        with pytest.raises(RuntimeError, match="replay"):
            _raw_socket_request(socket_path, request)
    finally:
        _stop(restarted)


def test_socket_authority_allows_only_one_concurrent_request_for_same_nonce(
    tmp_path: Path,
) -> None:
    keys_path, _key_id, _public_key = _manifest(tmp_path)
    process, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=8)
    request = {
        "version": 1,
        "operation": "sign",
        "namespace": _DAILY_STAGE_RECEIPT_NAMESPACE,
        "nonce": "3" * 64,
        "payload_sha256": hashlib.sha256(b"same-nonce").hexdigest(),
        "canonical_payload": base64.b64encode(b"same-nonce").decode("ascii"),
    }
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = tuple(
                pool.map(
                    lambda _index: _raw_socket_request_or_error(socket_path, request),
                    range(8),
                )
            )
        accepted = [item for item in results if isinstance(item, dict)]
        rejected = [item for item in results if isinstance(item, RuntimeError)]
        assert len(accepted) == 1
        assert len(rejected) == 7
        assert all("replay" in str(item) for item in rejected)
    finally:
        _stop(process)


def test_socket_authority_rejects_namespace_payload_nonce_and_replay_tampering(
    tmp_path: Path,
) -> None:
    keys_path, _key_id, _public_key = _manifest(tmp_path)
    process, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=5)
    try:
        nonce = "b" * 64
        request = {
            "version": 1,
            "operation": "sign",
            "namespace": _DAILY_STAGE_RECEIPT_NAMESPACE,
            "nonce": nonce,
            "payload_sha256": hashlib.sha256(b"payload").hexdigest(),
            "canonical_payload": base64.b64encode(b"payload").decode("ascii"),
        }
        _raw_socket_request(socket_path, request)
        with pytest.raises(RuntimeError, match="replay|nonce|failed"):
            _raw_socket_request(socket_path, request)

        bad_namespace = {**request, "nonce": "c" * 64, "namespace": "daily/forged"}
        with pytest.raises(RuntimeError, match="namespace|shape|failed"):
            _raw_socket_request(socket_path, bad_namespace)

        bad_payload = {
            **request,
            "nonce": "d" * 64,
            "payload_sha256": hashlib.sha256(b"payload").hexdigest(),
            "canonical_payload": base64.b64encode(b"tampered").decode("ascii"),
        }
        with pytest.raises(RuntimeError, match="payload|hash|failed"):
            _raw_socket_request(socket_path, bad_payload)
    finally:
        _stop(process)


@pytest.mark.parametrize(
    ("field", "tampered"),
    (
        ("nonce", "4" * 64),
        ("namespace", _DAILY_RUN_RECEIPT_NAMESPACE),
        ("payload_sha256", "5" * 64),
        ("key_id", "daily-v2"),
    ),
)
def test_socket_client_rejects_tampered_envelope_fields_with_valid_envelope_signature(
    tmp_path: Path,
    field: str,
    tampered: str,
) -> None:
    keys_path, key_id, public_key = _manifest(tmp_path)
    private_key = _private_key_from_manifest(keys_path)
    fake_socket = _short_socket_path(field)
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(fake_socket))
    server.listen(1)
    ready = threading.Event()

    def fake_server() -> None:
        ready.set()
        conn, _addr = server.accept()
        with conn:
            payload = _read_frame(conn)
            request = strict_json_loads(payload)
            assert isinstance(request, dict)
            response = {
                "version": 1,
                "operation": "sign",
                "namespace": request["namespace"],
                "nonce": request["nonce"],
                "payload_sha256": request["payload_sha256"],
                "key_id": key_id,
            }
            response[field] = tampered
            response["signature"] = _sign_bytes(
                private_key,
                base64.b64decode(str(request["canonical_payload"]), validate=True),
            )
            _write_frame(conn, canonical_json_bytes(response))
        server.close()

    thread = threading.Thread(target=fake_server, daemon=True)
    thread.start()
    assert ready.wait(timeout=2)
    client = DailyReceiptSocketSigningClient.for_test_endpoint(
        socket_path=fake_socket,
        active_key_id=key_id,
        active_public_key_pem=public_key,
        timeout_seconds=2,
        max_attempts=1,
    )
    with pytest.raises(RuntimeError, match="envelope|response|key"):
        client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"tampered-field")
    thread.join(timeout=2)


def test_socket_client_rejects_request_envelope_signature_for_expected_response_fields(
    tmp_path: Path,
) -> None:
    keys_path, key_id, public_key = _manifest(tmp_path)
    private_key = _private_key_from_manifest(keys_path)
    fake_socket = _short_socket_path("raw")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(fake_socket))
    server.listen(1)
    ready = threading.Event()

    def fake_server() -> None:
        ready.set()
        conn, _addr = server.accept()
        with conn:
            payload = _read_frame(conn)
            request = strict_json_loads(payload)
            assert isinstance(request, dict)
            response = {
                "version": 1,
                "operation": "sign",
                "namespace": request["namespace"],
                "nonce": request["nonce"],
                "payload_sha256": request["payload_sha256"],
                "key_id": key_id,
            }
            request_envelope = {
                "version": response["version"],
                "operation": response["operation"],
                "namespace": response["namespace"],
                "nonce": response["nonce"],
                "payload_sha256": response["payload_sha256"],
                "key_id": response["key_id"],
            }
            response["signature"] = _sign_bytes(
                private_key,
                canonical_json_bytes(request_envelope),
            )
            _write_frame(conn, canonical_json_bytes(response))
        server.close()

    thread = threading.Thread(target=fake_server, daemon=True)
    thread.start()
    assert ready.wait(timeout=2)
    client = DailyReceiptSocketSigningClient.for_test_endpoint(
        socket_path=fake_socket,
        active_key_id=key_id,
        active_public_key_pem=public_key,
        timeout_seconds=2,
        max_attempts=1,
    )
    with pytest.raises(RuntimeError, match="response"):
        client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"tampered-field")
    thread.join(timeout=2)


def test_socket_client_rejects_previous_key_response_and_nonce_mismatch(
    tmp_path: Path,
) -> None:
    keys_path, key_id, active_public, previous_key_id, previous_public = _rotated_manifest(tmp_path)
    process, socket_path = _start_server(tmp_path, keys_path=keys_path, max_connections=1)
    try:
        client = DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=socket_path,
            active_key_id=key_id,
            active_public_key_pem=active_public,
            previous_public_key_pems={previous_key_id: previous_public},
            timeout_seconds=2,
        )
        signature = client.sign(namespace=_DAILY_RUN_RECEIPT_NAMESPACE, payload=b"active")
        assert _valid_ed25519_signature(signature)
    finally:
        _stop(process)

    fake_socket = _short_socket_path("previous")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(fake_socket))
    server.listen(1)
    ready = threading.Event()

    def fake_server() -> None:
        ready.set()
        conn, _addr = server.accept()
        with conn:
            payload = _read_frame(conn)
            request = strict_json_loads(payload)
            response = {
                "version": 1,
                "operation": "sign",
                "namespace": request["namespace"],
                "nonce": "e" * 64,
                "payload_sha256": request["payload_sha256"],
                "key_id": previous_key_id,
                "signature": base64.b64encode(b"0" * 64).decode("ascii"),
            }
            _write_frame(conn, canonical_json_bytes(response))
        server.close()

    thread = threading.Thread(target=fake_server, daemon=True)
    thread.start()
    assert ready.wait(timeout=2)
    client = DailyReceiptSocketSigningClient.for_test_endpoint(
        socket_path=fake_socket,
        active_key_id=key_id,
        active_public_key_pem=active_public,
        previous_public_key_pems={previous_key_id: previous_public},
        timeout_seconds=2,
    )
    with pytest.raises(RuntimeError, match="nonce|active|response"):
        client.sign(namespace=_DAILY_STAGE_RECEIPT_NAMESPACE, payload=b"tamper")
    thread.join(timeout=2)


def test_production_client_has_no_path_helper_argv_or_env_injection() -> None:
    client = DailyReceiptSocketSigningClient.production(
        active_key_id="daily-v1",
        active_public_key_pem="public",
        timeout_seconds=5,
    )
    assert client.socket_path == DAILY_RECEIPT_SOCKET_ENDPOINT
    assert not hasattr(client, "command")
    assert not hasattr(client, "argv")
    assert not hasattr(client, "env")


def _raw_socket_request(socket_path: Path, request: dict[str, object]) -> dict[str, object]:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(2)
        client.connect(str(socket_path))
        _write_frame(client, canonical_json_bytes(request))
        payload = _read_frame(client)
    response = strict_json_loads(payload)
    if not isinstance(response, dict) or response.get("ok") is False:
        raise RuntimeError(str(response))
    return response


def _raw_socket_request_or_error(
    socket_path: Path,
    request: dict[str, object],
) -> dict[str, object] | RuntimeError:
    try:
        return _raw_socket_request(socket_path, request)
    except RuntimeError as exc:
        return exc


def _sign_bytes(private_key: Path, payload: bytes) -> str:
    payload_path = private_key.parent / f"{hashlib.sha256(payload).hexdigest()}.payload"
    payload_path.write_bytes(payload)
    payload_path.chmod(0o600)
    completed = subprocess.run(
        [
            _openssl(),
            "pkeyutl",
            "-sign",
            "-rawin",
            "-inkey",
            str(private_key),
            "-in",
            str(payload_path),
        ],
        check=True,
        capture_output=True,
    )
    return base64.b64encode(completed.stdout).decode("ascii")


def _write_frame(conn: socket.socket, payload: bytes) -> None:
    conn.sendall(len(payload).to_bytes(4, "big") + payload)


def _read_frame(conn: socket.socket) -> bytes:
    raw_size = conn.recv(4)
    if len(raw_size) != 4:
        raise RuntimeError("missing frame")
    remaining = int.from_bytes(raw_size, "big")
    chunks: list[bytes] = []
    while remaining:
        chunk = conn.recv(remaining)
        if not chunk:
            raise RuntimeError("truncated frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)
