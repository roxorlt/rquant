from __future__ import annotations

import inspect
import os
import socket
import tempfile
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

import rquant.source_broker_client as client_module
import rquant.source_broker_protocol as protocol_module
import rquant.source_broker_service as service_module
from rquant.adapter_manifest import (
    REPLAY_CLAIM_NAMESPACE,
    AdapterManifest,
    PydanticModelSchema,
    SourceUsePlan,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.source_broker import EffectReceipt, QuotaReservation, SourceBrokerError
from rquant.source_broker_client import SourceBrokerUnixClient, build_source_broker_client
from rquant.source_broker_protocol import (
    MAX_SOURCE_BROKER_FRAME_BYTES,
    DailyBarsCallRequest,
    PeerCredentialsPolicy,
    ServerCredentialsPolicy,
    SocketEndpointIdentity,
    SocketEndpointPolicy,
    SourceBrokerCallRequest,
    SourceBrokerCallResponse,
    SourceBrokerFinalizeRequest,
    SourceBrokerFinalizeResponse,
    SourceBrokerStartRequest,
    SourceBrokerStartResponse,
    SourceBrokerTransportError,
    decode_request,
    decode_response,
    encode_request,
    encode_response,
    read_frame,
    validate_response_binding,
)
from rquant.source_broker_service import SourceBrokerUnixService, _kernel_peer_credentials
from rquant.source_broker_transport_adapter import SourceBrokerTransportAdapter
from rquant.strict_json import canonical_json_bytes, strict_json_loads


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
        claim_token="claim-transport",
        audience="lab-broker-a",
        not_before=datetime(2026, 8, 5, tzinfo=UTC),
        expires_at=datetime(2026, 8, 5, tzinfo=UTC) + timedelta(minutes=10),
        nonce="nonce-transport",
        single_use_authority_id="global-source-use",
    )
    return unsigned.model_copy(update={"signature": "plan-signature"})


def _common(plan: SourceUsePlan, *, operation_id: str = "a" * 64) -> dict[str, object]:
    return {
        "operation_id": operation_id,
        "saga_id": "lab-source-saga",
        "attempt_identity_hash": "b" * 64,
        "plan_hash": plan.plan_hash,
        "plan": plan,
    }


def _start_request(plan: SourceUsePlan | None = None) -> SourceBrokerStartRequest:
    bound = _plan() if plan is None else plan
    return SourceBrokerStartRequest(operation="start", **_common(bound))


def _endpoint(path: Path) -> SocketEndpointPolicy:
    return SocketEndpointPolicy(
        path=path,
        owner_uid=os.geteuid(),
        group_gid=os.getegid(),
        mode=0o600,
    )


@pytest.fixture
def short_socket_directory() -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="rq-", dir="/private/tmp") as directory:
        yield Path(directory)


class _FakeBroker:
    def __init__(self) -> None:
        self.started: list[SourceUsePlan] = []

    def start(self, plan: SourceUsePlan) -> QuotaReservation:
        self.started.append(plan)
        return QuotaReservation(
            reservation_id="f" * 64,
            claim_token=plan.claim_token,
            source=plan.source or "missing",
            reserved_units=plan.cost_per_call * plan.max_calls,
        )

    def call(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("call was not expected")

    def finalize(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("finalize was not expected")


class _MemoryConnection:
    def __init__(self, incoming: bytes = b"") -> None:
        self.incoming = bytearray(incoming)
        self.sent = bytearray()
        self.events: list[str] = []
        self.closed = False

    def settimeout(self, _seconds: float) -> None:
        self.events.append("timeout")

    def connect(self, _path: str) -> None:
        self.events.append("connect")

    def recv(self, size: int) -> bytes:
        self.events.append("read")
        chunk = bytes(self.incoming[:size])
        del self.incoming[:size]
        return chunk

    def sendall(self, payload: bytes) -> None:
        self.events.append("write")
        self.sent.extend(payload)

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> _MemoryConnection:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class _NoAcceptListener:
    def __init__(self, listener: socket.socket, endpoint: Path) -> None:
        self._listener = listener
        self._endpoint = endpoint
        self.accepted = False

    @property
    def family(self) -> socket.AddressFamily:
        return self._listener.family

    def fileno(self) -> int:
        return self._listener.fileno()

    def getsockname(self) -> str:
        return str(self._endpoint)

    def getsockopt(self, level: int, option: int) -> int:
        if (level, option) == (socket.SOL_SOCKET, socket.SO_ACCEPTCONN):
            return 1
        return self._listener.getsockopt(level, option)

    def accept(self) -> tuple[object, object]:
        self.accepted = True
        raise AssertionError("unsafe directory authority reached accept")

    def __enter__(self) -> _NoAcceptListener:
        return self

    def __exit__(self, *_args: object) -> None:
        self._listener.close()


def _real_directory_authority_without_bind(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: SocketEndpointPolicy,
) -> tuple[_NoAcceptListener, object]:
    directory = service_module._open_directory_authority(endpoint)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    opened_listener = os.fstat(listener.fileno())
    endpoint_identity = SocketEndpointIdentity(
        device=101,
        inode=202,
        owner_uid=endpoint.owner_uid,
        group_gid=endpoint.group_gid,
        mode=endpoint.mode,
    )
    authority = service_module._ListenerAuthority(
        directory=directory,
        endpoint=endpoint_identity,
        listener_device=opened_listener.st_dev,
        listener_inode=opened_listener.st_ino,
    )
    monkeypatch.setattr(
        service_module,
        "_endpoint_identity_at",
        lambda _directory, _endpoint: endpoint_identity,
    )
    return _NoAcceptListener(listener, endpoint.path), authority


def test_wire_validates_strict_types_then_reencodes_exact_canonical_bytes() -> None:
    request = _start_request()
    assert decode_request(encode_request(request)) == request

    valid = request.model_dump(mode="json")
    for field, value in (
        ("schema_version", 1.0),
        ("schema_version", True),
        ("operation_id", f" {'a' * 64}"),
        ("saga_id", " lab-source-saga"),
        ("attempt_identity_hash", f"{'b' * 64} "),
    ):
        with pytest.raises(SourceBrokerTransportError, match="validation|canonical"):
            decode_request(canonical_json_bytes({**valid, field: value}))

    nested_trim = request.model_dump(mode="json")
    nested_trim["plan"]["issuer"] = " plan-authority "
    with pytest.raises(SourceBrokerTransportError, match="canonical"):
        decode_request(canonical_json_bytes(nested_trim))

    alias = request.model_dump(mode="json")
    alias["schemaVersion"] = alias.pop("schema_version")
    with pytest.raises(SourceBrokerTransportError, match="validation"):
        decode_request(canonical_json_bytes(alias))

    with pytest.raises(ValidationError):
        SourceBrokerStartRequest.model_validate({**valid, "schema_version": 1.0})
    with pytest.raises(ValidationError):
        SourceBrokerStartRequest.model_validate({**valid, "schema_version": True})


@pytest.mark.parametrize(
    "payload",
    (
        b'{"operation":"start","operation":"call"}',
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
        b'{"schema_version":-Infinity}',
    ),
)
def test_wire_rejects_duplicate_keys_and_nonfinite_numbers(payload: bytes) -> None:
    with pytest.raises(SourceBrokerTransportError, match="JSON is invalid"):
        decode_request(payload)


def test_wire_rejects_noncanonical_bytes_after_successful_validation() -> None:
    canonical = encode_request(_start_request())
    noncanonical = canonical.replace(b'":', b'": ', 1)

    with pytest.raises(SourceBrokerTransportError, match="exact validated canonical JSON"):
        decode_request(noncanonical)


def test_wire_has_a_closed_operation_union_and_typed_call_request() -> None:
    plan = _plan()
    start = SourceBrokerStartRequest(operation="start", **_common(plan))
    call = SourceBrokerCallRequest(
        operation="call",
        **_common(plan, operation_id="c" * 64),
        idempotency_key="daily-bars-20260805",
        call_request=DailyBarsCallRequest(
            request_type="daily_bars_v1",
            trade_date="2026-08-05",
            market="SZ",
        ),
    )
    finalize = SourceBrokerFinalizeRequest(
        operation="finalize",
        **_common(plan, operation_id="d" * 64),
    )

    assert type(decode_request(encode_request(start))) is SourceBrokerStartRequest
    assert type(decode_request(encode_request(call))) is SourceBrokerCallRequest
    assert type(decode_request(encode_request(finalize))) is SourceBrokerFinalizeRequest

    call_payload = call.model_dump(mode="json")
    for forbidden in ("credential", "module", "provider", "class"):
        with pytest.raises(SourceBrokerTransportError, match="validation"):
            decode_request(canonical_json_bytes({**call_payload, forbidden: "forbidden"}))
    with pytest.raises(ValidationError):
        SourceBrokerCallRequest.model_validate(
            {**call_payload, "idempotency_key": " daily-bars-20260805"}
        )
    with pytest.raises(ValidationError, match="plan_hash"):
        SourceBrokerStartRequest.model_validate({**start.model_dump(), "plan_hash": "e" * 64})


def test_closed_responses_are_canonical_and_bound_to_the_request() -> None:
    request = _start_request()
    response = SourceBrokerStartResponse.from_request(
        request=request,
        reservation=QuotaReservation(
            reservation_id="f" * 64,
            claim_token=request.plan.claim_token,
            source="tushare",
            reserved_units=2,
        ),
    )

    assert decode_response(encode_response(response)) == response
    validate_response_binding(request=request, response=response)
    with pytest.raises(SourceBrokerTransportError, match="binding"):
        validate_response_binding(
            request=request,
            response=response.model_copy(update={"plan_hash": "e" * 64}),
        )
    payload = response.model_dump(mode="json")
    payload["reservation"]["reserved_units"] = 2.0
    with pytest.raises(SourceBrokerTransportError, match="validation|canonical"):
        decode_response(canonical_json_bytes(payload))


def test_frame_reader_rejects_oversized_negative_and_truncated_frames() -> None:
    for header in (
        (MAX_SOURCE_BROKER_FRAME_BYTES + 1).to_bytes(4, "big"),
        (-1).to_bytes(4, "big", signed=True),
    ):
        receiver, sender = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sender.sendall(header)
            with pytest.raises(SourceBrokerTransportError, match="frame size"):
                read_frame(receiver)
        finally:
            receiver.close()
            sender.close()

    receiver, sender = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sender.sendall((5).to_bytes(4, "big") + b"cut")
        sender.shutdown(socket.SHUT_WR)
        with pytest.raises(SourceBrokerTransportError, match="truncated"):
            read_frame(receiver)
    finally:
        receiver.close()
        sender.close()


def test_client_authenticates_connected_server_and_rejects_inflight_replacement(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    connection = _MemoryConnection()
    identity = SocketEndpointIdentity(1, 2, os.geteuid(), os.getegid(), 0o600)
    checks = 0

    def validate(*_args: object, **_kwargs: object) -> SocketEndpointIdentity:
        nonlocal checks
        checks += 1
        if checks == 2:
            raise SourceBrokerTransportError("source broker socket endpoint was replaced")
        return identity

    monkeypatch.setattr(client_module, "require_linux_source_broker_transport", lambda: None)
    monkeypatch.setattr(client_module, "validate_socket_endpoint", validate)
    monkeypatch.setattr(client_module.socket, "socket", lambda *_args: connection)
    monkeypatch.setattr(
        client_module,
        "_kernel_peer_credentials",
        lambda _connection: (300, os.geteuid(), os.getegid()),
    )
    authenticated: list[tuple[int, SocketEndpointIdentity]] = []
    monkeypatch.setattr(
        client_module,
        "verify_connected_server_authority",
        lambda **values: authenticated.append((values["server_pid"], values["endpoint_identity"])),
    )
    client = SourceBrokerUnixClient(
        endpoint=_endpoint(tmp_path / "source-broker.sock"),
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
            expected_pid=300,
        ),
        timeout_seconds=1,
        max_attempts=1,
    )

    with pytest.raises(SourceBrokerTransportError, match="failed"):
        client.execute(_start_request())

    assert authenticated == [(300, identity)]
    assert connection.events == ["timeout", "connect"]


def test_client_rejects_wrong_connected_server_credentials_before_write(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    connection = _MemoryConnection()
    identity = SocketEndpointIdentity(1, 2, os.geteuid(), os.getegid(), 0o600)
    monkeypatch.setattr(client_module, "require_linux_source_broker_transport", lambda: None)
    monkeypatch.setattr(client_module, "validate_socket_endpoint", lambda *_args, **_kw: identity)
    monkeypatch.setattr(client_module.socket, "socket", lambda *_args: connection)
    monkeypatch.setattr(
        client_module,
        "_kernel_peer_credentials",
        lambda _connection: (301, os.geteuid() + 1, os.getegid()),
    )
    client = SourceBrokerUnixClient(
        endpoint=_endpoint(tmp_path / "source-broker.sock"),
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        ),
        timeout_seconds=1,
        max_attempts=1,
    )

    with pytest.raises(SourceBrokerTransportError, match="failed"):
        client.execute(_start_request())

    assert "write" not in connection.events


def test_service_checks_kernel_credentials_before_read_and_denied_peer_never_reads(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    request = _start_request()
    frame = len(encode_request(request)).to_bytes(4, "big") + encode_request(request)
    broker = _FakeBroker()
    service = SourceBrokerUnixService(
        endpoint=_endpoint(tmp_path / "source-broker.sock"),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.geteuid()}),
            allowed_gids=frozenset({os.getegid()}),
        ),
        adapter=SourceBrokerTransportAdapter(broker),
        connection_timeout_seconds=1,
    )
    allowed = _MemoryConnection(frame)

    def credentials(_connection: object) -> tuple[int, int, int]:
        allowed.events.append("credentials")
        return 300, os.geteuid(), os.getegid()

    monkeypatch.setattr(service_module, "_kernel_peer_credentials", credentials)
    service._serve_connection(allowed)
    assert allowed.events.index("credentials") < allowed.events.index("read")
    assert broker.started == [request.plan]
    response_size = int.from_bytes(allowed.sent[:4], "big")
    decoded = decode_response(bytes(allowed.sent[4 : 4 + response_size]))
    assert isinstance(decoded, SourceBrokerStartResponse)

    denied = _MemoryConnection()
    monkeypatch.setattr(
        service_module,
        "_kernel_peer_credentials",
        lambda _connection: (301, os.geteuid() + 1, os.getegid()),
    )
    service._serve_connection(denied)
    assert "read" not in denied.events
    assert "write" in denied.events


def test_service_revalidates_listener_authority_after_accept_before_read(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    connection = _MemoryConnection()

    class FakeListener:
        def __enter__(self) -> FakeListener:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def accept(self) -> tuple[_MemoryConnection, None]:
            return connection, None

    service = SourceBrokerUnixService(
        endpoint=_endpoint(tmp_path / "source-broker.sock"),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.geteuid()}),
            allowed_gids=frozenset({os.getegid()}),
        ),
        adapter=SourceBrokerTransportAdapter(_FakeBroker()),
        connection_timeout_seconds=1,
    )
    authority = SimpleNamespace(directory_fd=99)
    validations = 0

    def validate(*_args: object, **_kwargs: object) -> None:
        nonlocal validations
        validations += 1
        if validations == 2:
            raise SourceBrokerTransportError("listener authority was replaced")

    monkeypatch.setattr(service_module, "require_linux_source_broker_transport", lambda: None)
    monkeypatch.setattr(service, "_bind_listener", lambda: (FakeListener(), authority))
    monkeypatch.setattr(service_module, "_validate_listener_authority", validate)
    monkeypatch.setattr(service, "_cleanup_listener_authority", lambda *_args: None)

    with pytest.raises(SourceBrokerTransportError, match="replaced"):
        service.serve(max_connections=1)
    assert validations == 2
    assert connection.closed
    assert "read" not in connection.events


def test_service_rejects_real_directory_mode_downgrade_before_accept(
    monkeypatch: pytest.MonkeyPatch,
    short_socket_directory: Path,
) -> None:
    socket_directory = short_socket_directory
    service = SourceBrokerUnixService(
        endpoint=_endpoint(socket_directory / "source-broker.sock"),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.geteuid()}),
            allowed_gids=frozenset({os.getegid()}),
        ),
        adapter=SourceBrokerTransportAdapter(_FakeBroker()),
        connection_timeout_seconds=1,
    )
    monkeypatch.setattr(service_module, "require_linux_source_broker_transport", lambda: None)
    guarded_listener, authority = _real_directory_authority_without_bind(
        monkeypatch,
        service._endpoint,
    )
    monkeypatch.setattr(service, "_bind_listener", lambda: (guarded_listener, authority))
    monkeypatch.setattr(
        service,
        "_serve_connection",
        lambda _connection: pytest.fail("unsafe directory reached request handler"),
    )
    socket_directory.chmod(0o777)
    try:
        with pytest.raises(SourceBrokerTransportError, match="directory authority"):
            service.serve(max_connections=1)
    finally:
        socket_directory.chmod(0o700)

    assert not guarded_listener.accepted


@pytest.mark.parametrize("field", ("st_uid", "st_gid"))
def test_service_rejects_pinned_directory_owner_change_before_accept(
    monkeypatch: pytest.MonkeyPatch,
    short_socket_directory: Path,
    field: str,
) -> None:
    socket_directory = short_socket_directory
    service = SourceBrokerUnixService(
        endpoint=_endpoint(socket_directory / "source-broker.sock"),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.geteuid()}),
            allowed_gids=frozenset({os.getegid()}),
        ),
        adapter=SourceBrokerTransportAdapter(_FakeBroker()),
        connection_timeout_seconds=1,
    )
    monkeypatch.setattr(service_module, "require_linux_source_broker_transport", lambda: None)
    guarded_listener, authority = _real_directory_authority_without_bind(
        monkeypatch,
        service._endpoint,
    )
    monkeypatch.setattr(service, "_bind_listener", lambda: (guarded_listener, authority))
    monkeypatch.setattr(
        service,
        "_serve_connection",
        lambda _connection: pytest.fail("unsafe directory reached request handler"),
    )
    real_fstat = service_module.os.fstat

    def changed_directory_owner(descriptor: int) -> os.stat_result | SimpleNamespace:
        observed = real_fstat(descriptor)
        if descriptor != authority.directory.fd:
            return observed
        values = {
            "st_dev": observed.st_dev,
            "st_ino": observed.st_ino,
            "st_mode": observed.st_mode,
            "st_uid": observed.st_uid,
            "st_gid": observed.st_gid,
        }
        values[field] += 1
        return SimpleNamespace(**values)

    monkeypatch.setattr(service_module.os, "fstat", changed_directory_owner)

    with pytest.raises(SourceBrokerTransportError, match="directory authority"):
        service.serve(max_connections=1)

    assert not guarded_listener.accepted


def test_darwin_refuses_client_connect_and_service_bind_before_socket_creation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class UnexpectedSocket:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("Darwin must fail before socket construction")

    monkeypatch.setattr(protocol_module.sys, "platform", "darwin")
    monkeypatch.setattr(client_module.socket, "socket", UnexpectedSocket)
    monkeypatch.setattr(service_module.socket, "socket", UnexpectedSocket)
    client = SourceBrokerUnixClient(
        endpoint=_endpoint(tmp_path / "source-broker.sock"),
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        ),
        timeout_seconds=1,
        max_attempts=1,
    )
    service = SourceBrokerUnixService(
        endpoint=_endpoint(tmp_path / "source-broker.sock"),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.geteuid()}),
            allowed_gids=frozenset({os.getegid()}),
        ),
        adapter=SourceBrokerTransportAdapter(_FakeBroker()),
    )

    with pytest.raises(SourceBrokerTransportError, match="Linux"):
        client.execute(_start_request())
    with pytest.raises(SourceBrokerTransportError, match="Linux"):
        service.serve(max_connections=1)


def test_offline_path_does_not_construct_a_transport_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        client_module,
        "SourceBrokerUnixClient",
        lambda *_args, **_kwargs: pytest.fail("offline path constructed a client"),
    )
    assert (
        build_source_broker_client(
            offline=True,
            endpoint=_endpoint(tmp_path / "source-broker.sock"),
            server_policy=ServerCredentialsPolicy(
                expected_uid=os.geteuid(),
                expected_gid=os.getegid(),
            ),
            timeout_seconds=1,
            max_attempts=1,
        )
        is None
    )


def test_service_and_adapter_have_no_generic_callback_or_credential_seam() -> None:
    service_parameters = inspect.signature(SourceBrokerUnixService).parameters
    adapter_parameters = inspect.signature(SourceBrokerTransportAdapter).parameters
    forbidden = ("application", "callback", "module", "provider", "credential")

    assert not any(fragment in name for name in service_parameters for fragment in forbidden)
    assert tuple(adapter_parameters) == ("broker",)
    assert "SO_PEERCRED" in inspect.getsource(_kernel_peer_credentials)


def test_closed_adapter_calls_existing_source_broker_end_to_end(tmp_path: Path) -> None:
    from tests.unit.test_adapter_manifest import (
        DailyRequest,
        create_test_authorities,
        signed_plan,
    )
    from tests.unit.test_source_broker import (
        EchoProvider,
        IdempotentQuotaLedger,
        _broker,
    )

    authorities = create_test_authorities(tmp_path / "keys")

    class MemoryReplayAuthority:
        authority_id = "global-source-use"

        def __init__(self) -> None:
            self._receipts: dict[str, tuple[tuple[str, ...], EffectReceipt]] = {}

        @property
        def lineage_verifier_fingerprints(self) -> frozenset[str]:
            return frozenset({"0" * 64})

        def consume_once(
            self,
            *,
            operation_id: str,
            nonce: str,
            plan_hash: str,
            claim_token: str,
            broker_id: str,
        ) -> EffectReceipt:
            binding = (operation_id, nonce, plan_hash, claim_token, broker_id)
            existing = self._receipts.get(claim_token)
            if existing is not None:
                if existing[0] != binding:
                    raise SourceBrokerError("replay claim binding conflicts")
                return existing[1]
            payload = {
                "nonce": nonce,
                "plan_hash": plan_hash,
                "claim_token": claim_token,
                "broker_id": broker_id,
            }
            unsigned = EffectReceipt(
                authority_id=self.authority_id,
                operation_id=operation_id,
                payload_hash=canonical_sha256(payload),
                effect="replay",
                outcome="applied",
                result_hash=canonical_sha256(None),
                key_id=authorities.replay.key_id,
                signature="",
            )
            receipt = unsigned.model_copy(
                update={
                    "signature": authorities.replay.sign(
                        namespace=REPLAY_CLAIM_NAMESPACE,
                        payload=unsigned.signing_bytes(),
                    )
                }
            )
            self._receipts[claim_token] = (binding, receipt)
            return receipt

        def verify_claim_binding(
            self,
            *,
            operation_id: str,
            nonce: str,
            plan_hash: str,
            claim_token: str,
            broker_id: str,
            receipt: EffectReceipt,
        ) -> EffectReceipt:
            stored = self.consume_once(
                operation_id=operation_id,
                nonce=nonce,
                plan_hash=plan_hash,
                claim_token=claim_token,
                broker_id=broker_id,
            )
            if stored != receipt:
                raise SourceBrokerError("replay claim receipt does not match")
            return stored

    provider = EchoProvider()
    broker = _broker(
        tmp_path / "broker.sqlite3",
        authorities=authorities,
        ledger=IdempotentQuotaLedger(authorities),
        replay=MemoryReplayAuthority(),  # type: ignore[arg-type]
        provider=provider,
    )
    adapter = SourceBrokerTransportAdapter(broker)
    plan = signed_plan(authorities)

    start = SourceBrokerStartRequest(operation="start", **_common(plan))
    started = adapter.handle(start)
    assert isinstance(started, SourceBrokerStartResponse)
    assert started.reservation.claim_token == plan.claim_token

    call = SourceBrokerCallRequest(
        operation="call",
        **_common(plan, operation_id="c" * 64),
        idempotency_key="daily-bars-20260805",
        call_request=DailyBarsCallRequest(
            request_type="daily_bars_v1",
            trade_date="2026-08-05",
            market="SZ",
        ),
    )
    called = adapter.handle(call)
    replayed = adapter.handle(call)
    assert isinstance(called, SourceBrokerCallResponse)
    assert called.receipt == replayed.receipt
    assert called.receipt.signature
    assert provider.calls[0].request == DailyRequest(
        trade_date="2026-08-05",
        filters={"market": "SZ"},
    )
    assert len(provider.calls) == 1

    finalize = SourceBrokerFinalizeRequest(
        operation="finalize",
        **_common(plan, operation_id="d" * 64),
    )
    finalized = adapter.handle(finalize)
    assert isinstance(finalized, SourceBrokerFinalizeResponse)
    assert finalized.statement.plan_hash == plan.plan_hash
    assert finalized.statement.signature


def test_strict_json_fixture_itself_has_no_duplicate_keys() -> None:
    request = _start_request()
    assert strict_json_loads(encode_request(request)) == request.model_dump(mode="json")
