from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.adapter_manifest import AdapterManifest, PydanticModelSchema, SourceUsePlan
from rquant.source_broker_client import SourceBrokerUnixClient
from rquant.source_broker_protocol import (
    ServerCredentialsPolicy,
    SocketEndpointPolicy,
    SourceBrokerStartRequest,
    SourceBrokerStartResponse,
    SourceBrokerTransportError,
    SourceBrokerTransportRemoteError,
    read_frame,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or not hasattr(socket, "SO_PEERCRED"),
    reason="requires a real Linux SO_PEERCRED and /proc listener authority gate",
)


def _plan() -> SourceUsePlan:
    request_schema = PydanticModelSchema(
        model_name="tests.DailyRequest",
        schema_hash="1" * 64,
    )
    response_schema = PydanticModelSchema(
        model_name="tests.DailyResponse",
        schema_hash="2" * 64,
    )
    manifest = AdapterManifest(
        issuer="release-authority",
        key_id="manifest-v1",
        signature="manifest-signature",
        adapter_id="research.daily-bars",
        adapter_version="1.0.0",
        adapter_code_hash="3" * 64,
        network="provider",
        source="tushare",
        operation="daily_bars",
        cost_per_call=1,
        max_calls=2,
        request_schema=request_schema,
        response_schema=response_schema,
    )
    unsigned = SourceUsePlan.from_manifest(
        manifest,
        issuer="plan-authority",
        key_id="plan-v1",
        claim_token="claim-linux-transport",
        audience="lab-broker-a",
        not_before=datetime(2026, 8, 5, tzinfo=UTC),
        expires_at=datetime(2026, 8, 5, tzinfo=UTC) + timedelta(minutes=10),
        nonce="nonce-linux-transport",
        single_use_authority_id="global-source-use",
    )
    return unsigned.model_copy(update={"signature": "plan-signature"})


def _request() -> SourceBrokerStartRequest:
    plan = _plan()
    return SourceBrokerStartRequest(
        operation="start",
        operation_id="a" * 64,
        saga_id="lab-source-saga",
        attempt_identity_hash="b" * 64,
        plan_hash=plan.plan_hash,
        plan=plan,
    )


def _endpoint(path: Path) -> SocketEndpointPolicy:
    return SocketEndpointPolicy(
        path=path,
        owner_uid=os.geteuid(),
        group_gid=os.getegid(),
        mode=0o600,
    )


def _client(
    endpoint: SocketEndpointPolicy,
    process: subprocess.Popen[bytes],
    *,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> SourceBrokerUnixClient:
    return SourceBrokerUnixClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.geteuid() if expected_uid is None else expected_uid,
            expected_gid=os.getegid() if expected_gid is None else expected_gid,
            expected_pid=process.pid,
        ),
        timeout_seconds=2,
        max_attempts=1,
    )


def _start_service(
    tmp_path: Path,
    *,
    endpoint: SocketEndpointPolicy,
    allowed_uid: int,
    allowed_gid: int,
    max_connections: int,
    calls_path: Path,
) -> subprocess.Popen[bytes]:
    script = tmp_path / f"serve-{time.time_ns()}.py"
    script.write_text(
        "\n".join(
            (
                "from pathlib import Path",
                "from rquant.source_broker import QuotaReservation",
                "from rquant.source_broker_protocol import (",
                "    PeerCredentialsPolicy, SocketEndpointPolicy,",
                ")",
                "from rquant.source_broker_service import SourceBrokerUnixService",
                "from rquant.source_broker_transport_adapter import SourceBrokerTransportAdapter",
                "endpoint = SocketEndpointPolicy("
                f"path=Path({str(endpoint.path)!r}), owner_uid={endpoint.owner_uid}, "
                f"group_gid={endpoint.group_gid}, mode={endpoint.mode})",
                f"peer_policy = PeerCredentialsPolicy(allowed_uids=frozenset({{{allowed_uid}}}), "
                f"allowed_gids=frozenset({{{allowed_gid}}}))",
                f"calls_path = Path({str(calls_path)!r})",
                "class Broker:",
                "    def start(self, plan):",
                "        with calls_path.open('a', encoding='utf-8') as stream:",
                "            stream.write(plan.plan_hash + '\\n')",
                "        return QuotaReservation(",
                "            reservation_id='f' * 64, claim_token=plan.claim_token,",
                "            source=plan.source,",
                "            reserved_units=plan.cost_per_call * plan.max_calls,",
                "        )",
                "    def call(self, *args, **kwargs):",
                "        raise AssertionError('call was not expected')",
                "    def finalize(self, *args, **kwargs):",
                "        raise AssertionError('finalize was not expected')",
                "service = SourceBrokerUnixService(",
                "    endpoint=endpoint, peer_policy=peer_policy,",
                "    adapter=SourceBrokerTransportAdapter(Broker()),",
                "    connection_timeout_seconds=2,",
                ")",
                f"service.serve(max_connections={max_connections})",
            )
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
                f"source broker service exited early: {process.returncode}\n{stdout!r}\n{stderr!r}"
            )
        if endpoint.path.exists():
            return process
        time.sleep(0.02)
    process.kill()
    stdout, stderr = process.communicate(timeout=1)
    raise AssertionError(f"source broker service did not start\n{stdout!r}\n{stderr!r}")


def _finish(process: subprocess.Popen[bytes]) -> None:
    stdout, stderr = process.communicate(timeout=5)
    assert process.returncode == 0, f"source broker service failed\n{stdout!r}\n{stderr!r}"


def _stop(process: subprocess.Popen[bytes]) -> None:
    process.terminate()
    try:
        process.communicate(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=3)


def test_linux_roundtrip_authenticates_both_client_and_server(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path / "source-broker.sock")
    calls_path = tmp_path / "calls.log"
    process = _start_service(
        tmp_path,
        endpoint=endpoint,
        allowed_uid=os.geteuid(),
        allowed_gid=os.getegid(),
        max_connections=1,
        calls_path=calls_path,
    )

    response = _client(endpoint, process).execute(_request())

    assert isinstance(response, SourceBrokerStartResponse)
    assert response.operation_id == "a" * 64
    assert calls_path.read_text(encoding="utf-8").splitlines() == [_plan().plan_hash]
    _finish(process)


@pytest.mark.parametrize("forged", ("uid", "gid"))
def test_linux_service_rejects_incoming_peer_outside_uid_or_gid_policy(
    tmp_path: Path,
    forged: str,
) -> None:
    endpoint = _endpoint(tmp_path / f"source-broker-{forged}.sock")
    process = _start_service(
        tmp_path,
        endpoint=endpoint,
        allowed_uid=os.geteuid() + 1 if forged == "uid" else os.geteuid(),
        allowed_gid=os.getegid() + 1 if forged == "gid" else os.getegid(),
        max_connections=1,
        calls_path=tmp_path / "calls.log",
    )

    with pytest.raises(SourceBrokerTransportRemoteError, match="peer credentials are not allowed"):
        _client(endpoint, process).execute(_request())
    _finish(process)


@pytest.mark.parametrize("forged", ("uid", "gid"))
def test_linux_client_rejects_connected_server_outside_uid_or_gid_policy(
    tmp_path: Path,
    forged: str,
) -> None:
    endpoint = _endpoint(tmp_path / f"source-broker-server-{forged}.sock")
    process = _start_service(
        tmp_path,
        endpoint=endpoint,
        allowed_uid=os.geteuid(),
        allowed_gid=os.getegid(),
        max_connections=1,
        calls_path=tmp_path / "calls.log",
    )
    client = _client(
        endpoint,
        process,
        expected_uid=os.geteuid() + 1 if forged == "uid" else None,
        expected_gid=os.getegid() + 1 if forged == "gid" else None,
    )

    with pytest.raises(SourceBrokerTransportError, match="failed"):
        client.execute(_request())
    _finish(process)


def test_replay_after_disconnect_and_restart_has_no_transport_result_cache(
    tmp_path: Path,
) -> None:
    endpoint = _endpoint(tmp_path / "source-broker.sock")
    request = _request()
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(endpoint.path))
    os.chmod(endpoint.path, endpoint.mode)
    listener.listen(1)

    def drop_first_request() -> None:
        connection, _address = listener.accept()
        with connection:
            read_frame(connection)
        listener.close()
        endpoint.path.unlink(missing_ok=True)

    dropper = threading.Thread(target=drop_first_request, daemon=True)
    dropper.start()
    raw_client = SourceBrokerUnixClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
            expected_pid=os.getpid(),
        ),
        timeout_seconds=2,
        max_attempts=1,
    )
    with pytest.raises(SourceBrokerTransportError, match="failed|truncated"):
        raw_client.execute(request)
    dropper.join(timeout=3)
    assert not dropper.is_alive()

    calls_path = tmp_path / "calls.log"
    first = _start_service(
        tmp_path,
        endpoint=endpoint,
        allowed_uid=os.geteuid(),
        allowed_gid=os.getegid(),
        max_connections=1,
        calls_path=calls_path,
    )
    assert _client(endpoint, first).execute(request).operation_id == request.operation_id
    _finish(first)

    restarted = _start_service(
        tmp_path,
        endpoint=endpoint,
        allowed_uid=os.geteuid(),
        allowed_gid=os.getegid(),
        max_connections=1,
        calls_path=calls_path,
    )
    assert _client(endpoint, restarted).execute(request).operation_id == request.operation_id
    _finish(restarted)
    assert calls_path.read_text(encoding="utf-8").splitlines() == [
        request.plan_hash,
        request.plan_hash,
    ]


def test_linux_client_refuses_symlink_replacing_live_service_endpoint(tmp_path: Path) -> None:
    endpoint = _endpoint(tmp_path / "source-broker.sock")
    process = _start_service(
        tmp_path,
        endpoint=endpoint,
        allowed_uid=os.geteuid(),
        allowed_gid=os.getegid(),
        max_connections=1,
        calls_path=tmp_path / "calls.log",
    )
    target = tmp_path / "replacement"
    target.write_text("not a socket", encoding="utf-8")
    endpoint.path.unlink()
    endpoint.path.symlink_to(target)
    try:
        with pytest.raises(SourceBrokerTransportError, match="symlink|failed"):
            _client(endpoint, process).execute(_request())
    finally:
        _stop(process)
