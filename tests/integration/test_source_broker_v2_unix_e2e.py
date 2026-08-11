from __future__ import annotations

# ruff: noqa: E501,I001

import os
import signal
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker_protocol import (
    ServerCredentialsPolicy,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
)
from rquant.source_broker_v2 import (
    SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
    SourceAuthorityKeyring,
    SourceBrokerV2ClaimOnceRequest,
    SourceBrokerV2ClaimOnceResponse,
    SourceBrokerV2DispatchEnvelope,
    SourceBrokerV2DispatchRequest,
    SourceBrokerV2DispatchResponse,
    SourceBrokerV2OutboxPhase,
    SourceBrokerV2ReplayRequest,
    SourceBrokerV2ReplayResponse,
    SourceBrokerV2ReplayStatus,
    SourceBrokerV2SagaConflictError,
    SourceBrokerV2SagaUnavailableError,
    SourceBrokerV2UnixClient,
    SourceBrokerV2WireRequest,
)
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)
from rquant.source_broker_v2_service import (
    ExternalDispatchAuthorityResponse,
    ExternalDispatchCompleteRequest,
    ExternalDispatchReserveRequest,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or not hasattr(socket, "SO_PEERCRED"),
    reason="requires Linux SO_PEERCRED and /proc listener authority gates",
)

NOW = datetime(2026, 8, 9, 4, tzinfo=UTC)


class _SqliteExternalDispatchAuthority:
    """Explicit non-production authority kept outside the daemon's rollbackable ledger."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS authority_operation("
                "operation_id TEXT PRIMARY KEY, request_binding_hash TEXT NOT NULL, "
                "authority_generation INTEGER NOT NULL UNIQUE, authority_fence TEXT NOT NULL UNIQUE, "
                "result_json TEXT, result_hash TEXT)"
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    def reserve(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        assert deadline is None or deadline > time.monotonic()
        request = strict_model_validate_canonical_json(ExternalDispatchReserveRequest, payload)
        binding = request.request_binding_hash
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM authority_operation WHERE operation_id = ?",
                (request.operation_id,),
            ).fetchone()
            if row is None:
                generation = int(
                    connection.execute(
                        "SELECT COALESCE(MAX(authority_generation), 0) + 1 FROM authority_operation"
                    ).fetchone()[0]
                )
                fence = canonical_sha256(
                    {"authority_generation": generation, "operation_id": request.operation_id}
                )
                connection.execute(
                    "INSERT INTO authority_operation VALUES (?, ?, ?, ?, NULL, NULL)",
                    (request.operation_id, binding, generation, fence),
                )
                status = "absent"
                result_json = None
                result_hash = None
            else:
                if row["request_binding_hash"] != binding:
                    connection.rollback()
                    raise SourceBrokerTransportError("external authority binding conflict")
                generation = int(row["authority_generation"])
                fence = str(row["authority_fence"])
                result_json = row["result_json"]
                result_hash = row["result_hash"]
                status = "found" if result_json is not None else "unknown"
            connection.commit()
        return canonical_model_json_bytes(
            ExternalDispatchAuthorityResponse(
                operation="reserve",
                status=status,
                operation_id=request.operation_id,
                request_binding_hash=binding,
                authority_generation=generation,
                authority_fence=fence,
                result_json=result_json,
                result_hash=result_hash,
            )
        )

    def lookup(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        assert deadline is None or deadline > time.monotonic()
        request = strict_model_validate_canonical_json(ExternalDispatchReserveRequest, payload)
        binding = request.request_binding_hash
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM authority_operation WHERE operation_id = ?",
                (request.operation_id,),
            ).fetchone()
        if row is None:
            generation = 1
            fence = canonical_sha256(
                {
                    "authority_generation": generation,
                    "lookup": True,
                    "operation_id": request.operation_id,
                }
            )
            status = "absent"
            result_json = None
            result_hash = None
        else:
            if row["request_binding_hash"] != binding:
                raise SourceBrokerTransportError("external authority lookup binding conflict")
            generation = int(row["authority_generation"])
            fence = str(row["authority_fence"])
            result_json = row["result_json"]
            result_hash = row["result_hash"]
            status = "found" if result_json is not None else "unknown"
        return canonical_model_json_bytes(
            ExternalDispatchAuthorityResponse(
                operation="lookup",
                status=status,
                operation_id=request.operation_id,
                request_binding_hash=binding,
                authority_generation=generation,
                authority_fence=fence,
                result_json=result_json,
                result_hash=result_hash,
            )
        )

    def complete(self, payload: bytes, *, deadline: float | None = None) -> bytes:
        assert deadline is None or deadline > time.monotonic()
        request = strict_model_validate_canonical_json(ExternalDispatchCompleteRequest, payload)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM authority_operation WHERE operation_id = ?",
                (request.operation_id,),
            ).fetchone()
            if row is None or (
                row["request_binding_hash"] != request.request_binding_hash
                or int(row["authority_generation"]) != request.authority_generation
                or row["authority_fence"] != request.authority_fence
            ):
                connection.rollback()
                raise SourceBrokerTransportError("external authority completion conflict")
            if row["result_json"] is not None and (
                row["result_json"] != request.result_json
                or row["result_hash"] != request.result_hash
            ):
                connection.rollback()
                raise SourceBrokerTransportError("external authority terminal result conflict")
            connection.execute(
                "UPDATE authority_operation SET result_json = ?, result_hash = ? "
                "WHERE operation_id = ?",
                (request.result_json, request.result_hash, request.operation_id),
            )
            connection.commit()
        return canonical_model_json_bytes(
            ExternalDispatchAuthorityResponse(
                operation="complete",
                status="found",
                operation_id=request.operation_id,
                request_binding_hash=request.request_binding_hash,
                authority_generation=request.authority_generation,
                authority_fence=request.authority_fence,
                result_json=request.result_json,
                result_hash=request.result_hash,
            )
        )


def _openssl() -> str:
    executable = shutil.which("openssl")
    if executable is None:
        pytest.skip("openssl is required for SourceBroker v2 Unix E2E tests")
    return executable


def _keypair(root: Path, key_id: str) -> tuple[Path, Path, bytes]:
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    public_key = root / f"{key_id}.public.pem"
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
    return private_key, public_key, public_key.read_bytes()


def _payload_hash(payload: bytes) -> str:
    return canonical_sha256(strict_canonical_json_loads(payload))


def _dispatch_request(operation_id: str = "a" * 64) -> SourceBrokerV2DispatchRequest:
    payload = canonical_json_bytes({"symbol": "000001.SZ", "trade_date": "2026-08-07"})
    return SourceBrokerV2DispatchRequest(
        saga_id="saga-source-v2-e2e",
        operation_id=operation_id,
        call_id="daily-bars",
        attempt_identity_hash="1" * 64,
        claim_plan_hash="2" * 64,
        claim_binding_hash="3" * 64,
        manifest_hash="4" * 64,
        payload=payload,
        claim_payload_hash=_payload_hash(payload),
        dispatch_payload_hash=_payload_hash(payload),
    )


def _claim_request(request: SourceBrokerV2DispatchRequest) -> SourceBrokerV2ClaimOnceRequest:
    return SourceBrokerV2ClaimOnceRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="6" * 64,
        claim_binding_hash=request.claim_binding_hash,
        claim_generation=11,
        scheduler_fencing_token=29,
        executor_owner_token_hash="5" * 64,
        executor_generation=3,
        max_external_deadline=NOW + timedelta(seconds=10),
        not_before_takeover_at=NOW + timedelta(seconds=20),
    )


def _endpoint(path: Path) -> SocketEndpointPolicy:
    return SocketEndpointPolicy(
        path=path,
        owner_uid=os.getuid(),
        group_gid=os.getgid(),
        mode=0o600,
    )


def _keyring(public_key: bytes) -> SourceAuthorityKeyring:
    return SourceAuthorityKeyring(
        expected_authority_id="source-authority",
        allowed_public_keys={"current": public_key},
        expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE,
        expected_schema_version=2,
    )


def _client(
    endpoint: SocketEndpointPolicy,
    process: subprocess.Popen[bytes],
    keyring: SourceAuthorityKeyring,
    *,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> SourceBrokerV2UnixClient:
    return SourceBrokerV2UnixClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.getuid() if expected_uid is None else expected_uid,
            expected_gid=os.getgid() if expected_gid is None else expected_gid,
            expected_pid=process.pid,
        ),
        total_request_deadline_seconds=2,
        source_authority_keyring=keyring,
        max_attempts=1,
    )


def _write_server_script(
    tmp_path: Path,
    *,
    endpoint: SocketEndpointPolicy,
    ledger_path: Path,
    private_key: Path,
    public_key: Path,
    calls_path: Path,
    max_connections: int,
    allowed_uid: int | None = None,
    allowed_gid: int | None = None,
    provider_mode: str = "normal",
    deadline: float = 2.0,
) -> Path:
    script = tmp_path / f"serve-source-v2-{time.time_ns()}.py"
    script.write_text(
        "\n".join(
            (
                "from __future__ import annotations",
                "import os, signal, time",
                "from datetime import UTC, datetime",
                "from pathlib import Path",
                "from rquant.runtime_contracts import canonical_sha256",
                "from rquant.source_broker_protocol import PeerCredentialsPolicy, SocketEndpointPolicy",
                "from rquant.source_broker_v2 import SOURCE_BROKER_V2_AUTHORITY_PURPOSE, SourceAuthorityKeyring, SourceBrokerV2DispatchOutcome",
                "from rquant.source_broker_v2_server import SourceBrokerV2UnixService",
                "from rquant.source_broker_v2_service import OpenSslSourceBrokerV2AuthoritySigner, SourceBrokerV2ProviderDispatchResult, SourceBrokerV2ProviderService",
                "from rquant.strict_json import canonical_json_bytes",
                "from tests.integration.test_source_broker_v2_unix_e2e import _SqliteExternalDispatchAuthority",
                f"endpoint = SocketEndpointPolicy(path=Path({str(endpoint.path)!r}), owner_uid={endpoint.owner_uid}, group_gid={endpoint.group_gid}, mode={endpoint.mode})",
                f"ledger_path = Path({str(ledger_path)!r})",
                f"calls_path = Path({str(calls_path)!r})",
                f"provider_mode = {provider_mode!r}",
                "class Provider:",
                "    def dispatch(self, request):",
                "        calls = int(calls_path.read_text('utf-8')) if calls_path.exists() else 0",
                "        calls_path.write_text(str(calls + 1), encoding='utf-8')",
                "        if provider_mode == 'kill':",
                "            (calls_path.parent / 'provider-started').write_text('1', encoding='utf-8')",
                "            time.sleep(30)",
                "        if provider_mode == 'wait-file':",
                "            (calls_path.parent / 'provider-started').write_text('1', encoding='utf-8')",
                "            release_path = calls_path.parent / 'provider-release'",
                "            wait_deadline = time.monotonic() + 10",
                "            while not release_path.exists():",
                "                if time.monotonic() >= wait_deadline:",
                "                    raise TimeoutError('provider release file was not created')",
                "                time.sleep(0.02)",
                "        result = canonical_json_bytes({'calls': calls + 1, 'operation_id': request.operation_id})",
                "        receipt = canonical_json_bytes({'request_hash': request.request_hash})",
                "        return SourceBrokerV2ProviderDispatchResult(outcome=SourceBrokerV2DispatchOutcome.SUCCESS, response=result, transport_receipt=receipt)",
                "    def finalize(self, request):",
                "        raise AssertionError('finalize not used in E2E')",
                f"public_key = Path({str(public_key)!r}).read_bytes()",
                "keyring = SourceAuthorityKeyring(expected_authority_id='source-authority', allowed_public_keys={'current': public_key}, expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE, expected_schema_version=2)",
                f"signer = OpenSslSourceBrokerV2AuthoritySigner(authority_id='source-authority', key_id='current', private_key_path=Path({str(private_key)!r}))",
                "external_authority = _SqliteExternalDispatchAuthority(ledger_path.with_name(ledger_path.name + '.external.sqlite3'))",
                "provider_service = SourceBrokerV2ProviderService.create_for_test(ledger_path=ledger_path, provider=Provider(), authority_signer=signer, authority_keyring=keyring, external_dispatch_authority=external_authority, clock=lambda: datetime(2026, 8, 9, 4, tzinfo=UTC), profile='nonproduction')",
                "service = SourceBrokerV2UnixService(endpoint=endpoint, peer_policy=PeerCredentialsPolicy("
                f"allowed_uids=frozenset({{{os.getuid() if allowed_uid is None else allowed_uid}}}), "
                f"allowed_gids=frozenset({{{os.getgid() if allowed_gid is None else allowed_gid}}})), "
                f"provider_service=provider_service, total_request_deadline_seconds={deadline})",
                f"service.serve_forever(max_connections={max_connections + 1})",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    return script


def _start_server(script: Path, endpoint: SocketEndpointPolicy) -> subprocess.Popen[bytes]:
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
                f"SourceBroker v2 server exited early: {process.returncode}\n{stdout!r}\n{stderr!r}"
            )
        if _endpoint_accepts_probe_connection(endpoint):
            time.sleep(0.05)
            if process.poll() is not None:
                stdout, stderr = process.communicate(timeout=1)
                raise AssertionError(
                    "SourceBroker v2 server died after readiness probe: "
                    f"{process.returncode}\n{stdout!r}\n{stderr!r}"
                )
            return process
        time.sleep(0.02)
    process.kill()
    stdout, stderr = process.communicate(timeout=1)
    raise AssertionError(f"SourceBroker v2 server did not start\n{stdout!r}\n{stderr!r}")


def _endpoint_accepts_probe_connection(endpoint: SocketEndpointPolicy) -> bool:
    if not endpoint.path.exists():
        return False
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.1)
        try:
            probe.connect(str(endpoint.path))
        except OSError:
            return False
    return True


def _finish(process: subprocess.Popen[bytes]) -> None:
    stdout, stderr = process.communicate(timeout=5)
    assert process.returncode == 0, f"server failed\n{stdout!r}\n{stderr!r}"


def _stop(process: subprocess.Popen[bytes]) -> None:
    process.terminate()
    try:
        process.communicate(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=3)


def test_linux_start_server_requires_connectable_listener_not_only_endpoint_path(
    tmp_path: Path,
) -> None:
    endpoint = _endpoint(tmp_path / "fake-ready.sock")
    script = tmp_path / "fake-ready.py"
    script.write_text(
        "\n".join(
            (
                "from pathlib import Path",
                "import time",
                f"Path({str(endpoint.path)!r}).write_text('not a socket', encoding='utf-8')",
                "time.sleep(30)",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    process: subprocess.Popen[bytes] | None = None
    try:
        with pytest.raises(AssertionError, match="did not start"):
            process = _start_server(script, endpoint)
    finally:
        if process is not None and process.poll() is None:
            _stop(process)


def test_linux_unix_daemon_roundtrip_response_loss_replay_and_cleanup(tmp_path: Path) -> None:
    private_key, public_key_path, public_key = _keypair(tmp_path / "keys", "current")
    endpoint = _endpoint(tmp_path / "source-v2.sock")
    ledger_path = tmp_path / "ledger.sqlite3"
    calls_path = tmp_path / "calls.txt"
    keyring = _keyring(public_key)
    request = _dispatch_request()
    claim_request = _claim_request(request)

    script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=ledger_path,
        private_key=private_key,
        public_key=public_key_path,
        calls_path=calls_path,
        # _start_server consumes one readiness probe; the helper adds that budget.
        max_connections=3,
    )
    process = _start_server(script, endpoint)
    claim_client = _client(endpoint, process, keyring)
    claim = SourceBrokerV2ClaimOnceResponse.model_validate_json(
        claim_client.claim_once(canonical_model_json_bytes(claim_request))
    )
    keyring.require_verified_claim(request=claim_request, receipt=claim)
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    wire_request = SourceBrokerV2WireRequest(
        operation="dispatch",
        challenge="8" * 64,
        payload=envelope,
        payload_hash=_payload_hash(envelope),
    )
    raw_wire = canonical_model_json_bytes(wire_request)
    lost_response = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        lost_response.settimeout(0.5)
        lost_response.connect(str(endpoint.path))
        lost_response.sendall(len(raw_wire).to_bytes(4, "big") + raw_wire)
        lost_response.shutdown(socket.SHUT_RD)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if calls_path.exists() and calls_path.read_text(encoding="utf-8") == "1":
                break
            time.sleep(0.02)
        assert calls_path.exists()
        assert calls_path.read_text(encoding="utf-8") == "1"
    finally:
        lost_response.close()

    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="9" * 64,
    )
    replay_client = _client(endpoint, process, keyring)
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        replay_client.replay(canonical_model_json_bytes(replay_request))
    )

    assert replay.status is SourceBrokerV2ReplayStatus.FOUND
    keyring.require_verified_replay(request=replay_request, receipt=replay)
    assert replay.result is not None
    dispatch = SourceBrokerV2DispatchResponse.model_validate_json(replay.result)
    assert dispatch.request_hash == request.request_hash
    assert strict_canonical_json_loads(dispatch.response) == {
        "calls": 1,
        "operation_id": request.operation_id,
    }
    assert calls_path.read_text(encoding="utf-8") == "1"
    _finish(process)
    assert not endpoint.path.exists()


def test_linux_second_daemon_refuses_live_endpoint_and_first_stays_reachable(
    tmp_path: Path,
) -> None:
    private_key, public_key_path, public_key = _keypair(tmp_path / "keys", "current")
    endpoint = _endpoint(tmp_path / "double-start.sock")
    keyring = _keyring(public_key)
    first_script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=tmp_path / "ledger.sqlite3",
        private_key=private_key,
        public_key=public_key_path,
        calls_path=tmp_path / "calls.txt",
        max_connections=2,
    )
    first = _start_server(first_script, endpoint)
    second_script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=tmp_path / "second-ledger.sqlite3",
        private_key=private_key,
        public_key=public_key_path,
        calls_path=tmp_path / "second-calls.txt",
        max_connections=1,
    )
    second = subprocess.Popen(
        [sys.executable, str(second_script)],
        cwd=Path(__file__).resolve().parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and second.poll() is None:
            time.sleep(0.02)
        if second.poll() is None:
            second.kill()
            stdout, stderr = second.communicate(timeout=3)
            raise AssertionError(
                "second SourceBroker v2 server stayed alive after binding a live endpoint\n"
                f"{stdout!r}\n{stderr!r}"
            )
        second_stdout, second_stderr = second.communicate(timeout=1)
        assert second.returncode != 0, second_stdout
        assert b"live" in second_stderr or b"stale" in second_stderr

        request = _dispatch_request()
        claim_request = _claim_request(request)
        client = _client(endpoint, first, keyring)
        claim = SourceBrokerV2ClaimOnceResponse.model_validate_json(
            client.claim_once(canonical_model_json_bytes(claim_request))
        )
        keyring.require_verified_claim(request=claim_request, receipt=claim)
        _finish(first)
    finally:
        if second.poll() is None:
            _stop(second)
        if first.poll() is None:
            _stop(first)


def test_linux_failed_same_ledger_double_start_does_not_poison_active_invocation(
    tmp_path: Path,
) -> None:
    private_key, public_key_path, public_key = _keypair(tmp_path / "keys", "current")
    endpoint = _endpoint(tmp_path / "same-ledger-double-start.sock")
    ledger_path = tmp_path / "ledger.sqlite3"
    calls_path = tmp_path / "calls.txt"
    keyring = _keyring(public_key)
    request = _dispatch_request()
    claim_request = _claim_request(request)
    first_script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=ledger_path,
        private_key=private_key,
        public_key=public_key_path,
        calls_path=calls_path,
        max_connections=4,
        provider_mode="wait-file",
    )
    first = _start_server(first_script, endpoint)
    client = _client(endpoint, first, keyring)
    claim = SourceBrokerV2ClaimOnceResponse.model_validate_json(
        client.claim_once(canonical_model_json_bytes(claim_request))
    )
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    dispatch_results: list[bytes] = []
    dispatch_errors: list[BaseException] = []

    def dispatch() -> None:
        try:
            dispatch_results.append(client.dispatch(envelope))
        except BaseException as exc:  # pragma: no cover - asserted below
            dispatch_errors.append(exc)

    dispatch_thread = threading.Thread(target=dispatch)
    dispatch_thread.start()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not (tmp_path / "provider-started").exists():
        time.sleep(0.02)
    assert (tmp_path / "provider-started").exists()

    second_script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=ledger_path,
        private_key=private_key,
        public_key=public_key_path,
        calls_path=tmp_path / "second-calls.txt",
        max_connections=1,
    )
    second = subprocess.Popen(
        [sys.executable, str(second_script)],
        cwd=Path(__file__).resolve().parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and second.poll() is None:
            time.sleep(0.02)
        if second.poll() is None:
            second.kill()
            stdout, stderr = second.communicate(timeout=3)
            raise AssertionError(
                "second SourceBroker v2 server stayed alive after same-ledger double start\n"
                f"{stdout!r}\n{stderr!r}"
            )
        second_stdout, second_stderr = second.communicate(timeout=1)
        assert second.returncode != 0, second_stdout
        assert b"live" in second_stderr or b"stale" in second_stderr

        (tmp_path / "provider-release").write_text("1", encoding="utf-8")
        dispatch_thread.join(timeout=5)
        assert not dispatch_thread.is_alive()
        assert not dispatch_errors
        assert len(dispatch_results) == 1
        replay_request = SourceBrokerV2ReplayRequest(
            saga_id=request.saga_id,
            operation_id=request.operation_id,
            phase=SourceBrokerV2OutboxPhase.DISPATCH,
            operation_request_hash=request.request_hash,
            challenge="8" * 64,
        )
        replay = SourceBrokerV2ReplayResponse.model_validate_json(
            client.replay(canonical_model_json_bytes(replay_request))
        )
        assert replay.status is SourceBrokerV2ReplayStatus.FOUND
        assert replay.result == dispatch_results[0]
        assert calls_path.read_text(encoding="utf-8") == "1"
        _finish(first)
    finally:
        (tmp_path / "provider-release").write_text("1", encoding="utf-8")
        dispatch_thread.join(timeout=1)
        if second.poll() is None:
            _stop(second)
        if first.poll() is None:
            _stop(first)


def test_linux_rejects_wrong_peer_before_request_read(tmp_path: Path) -> None:
    private_key, public_key_path, public_key = _keypair(tmp_path / "keys", "current")
    endpoint = _endpoint(tmp_path / "wrong-peer.sock")
    script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=tmp_path / "ledger.sqlite3",
        private_key=private_key,
        public_key=public_key_path,
        calls_path=tmp_path / "calls.txt",
        max_connections=1,
        allowed_uid=os.getuid() + 1,
    )
    process = _start_server(script, endpoint)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.connect(str(endpoint.path))
        connection.sendall((1024).to_bytes(4, "big"))
        time.sleep(0.2)
        with pytest.raises((BrokenPipeError, ConnectionResetError, OSError)):
            connection.sendall(b"x" * 1024)
    finally:
        connection.close()
    _finish(process)
    assert not (tmp_path / "calls.txt").exists()


def test_linux_service_rejects_renamed_ancestor_symlink_and_does_not_cross_cleanup(
    tmp_path: Path,
) -> None:
    private_key, public_key_path, public_key = _keypair(tmp_path / "keys", "current")
    authority_root = tmp_path / "authority-root"
    socket_parent = authority_root / "run"
    socket_parent.mkdir(parents=True)
    endpoint = _endpoint(socket_parent / "replace.sock")
    script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=tmp_path / "ledger.sqlite3",
        private_key=private_key,
        public_key=public_key_path,
        calls_path=tmp_path / "calls.txt",
        max_connections=1,
    )
    process = _start_server(script, endpoint)
    renamed_root = tmp_path / "authority-root-renamed"
    authority_root.rename(renamed_root)
    authority_root.symlink_to(renamed_root, target_is_directory=True)
    try:
        with pytest.raises((SourceBrokerTransportError, SourceBrokerV2SagaUnavailableError)):
            _client(endpoint, process, _keyring(public_key)).claim_once(
                canonical_model_json_bytes(_claim_request(_dispatch_request()))
            )
    finally:
        _stop(process)
    assert authority_root.is_symlink()
    assert (renamed_root / "run" / "replace.sock").exists()


def test_linux_slowloris_half_read_is_closed_without_provider_call(tmp_path: Path) -> None:
    private_key, public_key_path, _public_key = _keypair(tmp_path / "keys", "current")
    endpoint = _endpoint(tmp_path / "slowloris.sock")
    script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=tmp_path / "ledger.sqlite3",
        private_key=private_key,
        public_key=public_key_path,
        calls_path=tmp_path / "calls.txt",
        max_connections=1,
        deadline=0.2,
    )
    process = _start_server(script, endpoint)
    connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        connection.connect(str(endpoint.path))
        connection.sendall((128).to_bytes(4, "big") + b"{")
        time.sleep(0.5)
        with pytest.raises((BrokenPipeError, ConnectionResetError, OSError)):
            connection.sendall(b"x" * 1024)
    finally:
        connection.close()
    _finish(process)
    assert not (tmp_path / "calls.txt").exists()


def test_linux_multiprocess_duplicate_clients_call_provider_once(tmp_path: Path) -> None:
    private_key, public_key_path, public_key = _keypair(tmp_path / "keys", "current")
    endpoint = _endpoint(tmp_path / "multi.sock")
    ledger_path = tmp_path / "ledger.sqlite3"
    calls_path = tmp_path / "calls.txt"
    script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=ledger_path,
        private_key=private_key,
        public_key=public_key_path,
        calls_path=calls_path,
        max_connections=7,
    )
    process = _start_server(script, endpoint)
    keyring = _keyring(public_key)
    request = _dispatch_request()
    claim_request = _claim_request(request)
    client = _client(endpoint, process, keyring)
    claim = SourceBrokerV2ClaimOnceResponse.model_validate_json(
        client.claim_once(canonical_model_json_bytes(claim_request))
    )
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    envelope_path = tmp_path / "envelope.bin"
    envelope_path.write_bytes(envelope)
    client_script = tmp_path / "client.py"
    client_script.write_text(
        "\n".join(
            (
                "from pathlib import Path",
                "import os, sys",
                "from rquant.source_broker_protocol import ServerCredentialsPolicy, SocketEndpointPolicy",
                "from rquant.source_broker_v2 import SOURCE_BROKER_V2_AUTHORITY_PURPOSE, SourceAuthorityKeyring, SourceBrokerV2UnixClient",
                f"endpoint = SocketEndpointPolicy(path=Path({str(endpoint.path)!r}), owner_uid={endpoint.owner_uid}, group_gid={endpoint.group_gid}, mode={endpoint.mode})",
                f"public_key = Path({str(public_key_path)!r}).read_bytes()",
                "keyring = SourceAuthorityKeyring(expected_authority_id='source-authority', allowed_public_keys={'current': public_key}, expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE, expected_schema_version=2)",
                "client = SourceBrokerV2UnixClient(endpoint=endpoint, server_policy=ServerCredentialsPolicy("
                f"expected_uid={os.getuid()}, expected_gid={os.getgid()}, expected_pid={process.pid}), "
                "total_request_deadline_seconds=2, source_authority_keyring=keyring, max_attempts=1)",
                f"sys.stdout.buffer.write(client.dispatch(Path({str(envelope_path)!r}).read_bytes()))",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    clients = [
        subprocess.Popen(
            [sys.executable, str(client_script)],
            cwd=Path(__file__).resolve().parents[2],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for _ in range(6)
    ]
    outputs = [client.communicate(timeout=5) for client in clients]
    assert all(client.returncode == 0 for client in clients), outputs
    assert len({stdout for stdout, _stderr in outputs}) == 1
    assert calls_path.read_text(encoding="utf-8") == "1"
    _finish(process)


def test_linux_kill_restart_after_provider_start_requires_reconcile_not_second_dispatch(
    tmp_path: Path,
) -> None:
    private_key, public_key_path, public_key = _keypair(tmp_path / "keys", "current")
    endpoint = _endpoint(tmp_path / "kill.sock")
    ledger_path = tmp_path / "ledger.sqlite3"
    calls_path = tmp_path / "calls.txt"
    keyring = _keyring(public_key)
    request = _dispatch_request()
    claim_request = _claim_request(request)

    first_script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=ledger_path,
        private_key=private_key,
        public_key=public_key_path,
        calls_path=calls_path,
        max_connections=2,
        provider_mode="kill",
    )
    first = _start_server(first_script, endpoint)
    client = _client(endpoint, first, keyring)
    claim = SourceBrokerV2ClaimOnceResponse.model_validate_json(
        client.claim_once(canonical_model_json_bytes(claim_request))
    )
    envelope = canonical_model_json_bytes(
        SourceBrokerV2DispatchEnvelope(request=request, claim_receipt=claim)
    )
    envelope_path = tmp_path / "envelope.bin"
    envelope_path.write_bytes(envelope)
    blocked = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "from pathlib import Path\n"
                "import os, sys\n"
                "from rquant.source_broker_protocol import ServerCredentialsPolicy, SocketEndpointPolicy, SourceBrokerTransportError\n"
                "from rquant.source_broker_v2 import SOURCE_BROKER_V2_AUTHORITY_PURPOSE, SourceAuthorityKeyring, SourceBrokerV2SagaConflictError, SourceBrokerV2SagaUnavailableError, SourceBrokerV2TransportDeadlineError, SourceBrokerV2UnixClient\n"
                f"endpoint=SocketEndpointPolicy(path=Path({str(endpoint.path)!r}), owner_uid={endpoint.owner_uid}, group_gid={endpoint.group_gid}, mode={endpoint.mode})\n"
                f"keyring=SourceAuthorityKeyring(expected_authority_id='source-authority', allowed_public_keys={{'current': Path({str(public_key_path)!r}).read_bytes()}}, expected_purpose=SOURCE_BROKER_V2_AUTHORITY_PURPOSE, expected_schema_version=2)\n"
                f"client=SourceBrokerV2UnixClient(endpoint=endpoint, server_policy=ServerCredentialsPolicy(expected_uid={os.getuid()}, expected_gid={os.getgid()}, expected_pid={first.pid}), total_request_deadline_seconds=5, source_authority_keyring=keyring, max_attempts=1)\n"
                "try:\n"
                f"    client.dispatch(Path({str(envelope_path)!r}).read_bytes())\n"
                "except (SourceBrokerTransportError, SourceBrokerV2SagaConflictError, SourceBrokerV2SagaUnavailableError, SourceBrokerV2TransportDeadlineError) as exc:\n"
                "    sys.stderr.write(f'expected-dispatch-interrupted:{type(exc).__name__}:{exc}\\n')\n"
                "    raise SystemExit(7)\n"
                "raise SystemExit('dispatch unexpectedly completed')\n"
            ),
        ],
        cwd=Path(__file__).resolve().parents[2],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and not (tmp_path / "provider-started").exists():
        time.sleep(0.02)
    assert (tmp_path / "provider-started").exists()
    assert first.poll() is None
    assert blocked.poll() is None
    first.kill()
    first_stdout, first_stderr = first.communicate(timeout=3)
    assert first.returncode == -signal.SIGKILL
    assert first_stdout == b""
    assert first_stderr == b""
    blocked_stdout, blocked_stderr = blocked.communicate(timeout=3)
    assert blocked.returncode == 7, (blocked_stdout, blocked_stderr)
    assert blocked_stdout == b""
    assert b"expected-dispatch-interrupted:" in blocked_stderr
    second_script = _write_server_script(
        tmp_path,
        endpoint=endpoint,
        ledger_path=ledger_path,
        private_key=private_key,
        public_key=public_key_path,
        calls_path=calls_path,
        max_connections=2,
    )
    second = _start_server(second_script, endpoint)
    second_client = _client(endpoint, second, keyring)
    replay_request = SourceBrokerV2ReplayRequest(
        saga_id=request.saga_id,
        operation_id=request.operation_id,
        phase=SourceBrokerV2OutboxPhase.DISPATCH,
        operation_request_hash=request.request_hash,
        challenge="9" * 64,
    )
    replay = SourceBrokerV2ReplayResponse.model_validate_json(
        second_client.replay(canonical_model_json_bytes(replay_request))
    )
    assert replay.status is SourceBrokerV2ReplayStatus.UNKNOWN
    with pytest.raises(SourceBrokerV2SagaConflictError, match="reconcile|required|unknown"):
        second_client.dispatch(envelope)
    assert calls_path.read_text(encoding="utf-8") == "1"
    _finish(second)
