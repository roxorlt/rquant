from __future__ import annotations

import importlib.util
import json
import os
import socket
import threading
from pathlib import Path
from typing import Any

import pytest

import rquant.source_broker_v2_authority as authority_module
import rquant.source_broker_v2_server as daemon_server_module
from rquant.source_broker_protocol import (
    PeerCredentialsPolicy,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
)
from rquant.source_broker_v2_runtime import (
    SourceBrokerV2IdentityMatrix,
    SourceBrokerV2ProcessIdentity,
    SourceBrokerV2ProcessRole,
    source_broker_v2_default_runtime,
)
from rquant.source_broker_v2_server import SourceBrokerV2UnixService
from rquant.source_broker_v2_service import SourceBrokerV2ProviderService
from rquant.source_quota_authority import SourceQuotaParentAuthority


def _identity(
    role: SourceBrokerV2ProcessRole,
    *,
    uid: int,
    gid: int,
) -> SourceBrokerV2ProcessIdentity:
    return SourceBrokerV2ProcessIdentity(role=role, uid=uid, gid=gid)


def _runtime(root: Path):
    role = SourceBrokerV2ProcessRole
    identities = SourceBrokerV2IdentityMatrix(
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
    return source_broker_v2_default_runtime(root=root, identities=identities)


def test_source_daemon_policy_is_the_exact_daemon_type_and_gates_before_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    policy = authority_module.compose_production_source_broker_v2_source_daemon_policy(runtime)

    assert type(policy) is PeerCredentialsPolicy
    assert policy.allowed_uids == frozenset({runtime.identities.scheduler_client.uid})
    assert policy.allowed_gids == frozenset({runtime.identities.scheduler_client.gid})

    endpoint = SocketEndpointPolicy(
        path=tmp_path / "source.sock",
        owner_uid=runtime.identities.source_daemon.uid,
        group_gid=runtime.identities.scheduler_client.gid,
        mode=0o660,
    )
    provider = object.__new__(SourceBrokerV2ProviderService)
    service = SourceBrokerV2UnixService(
        endpoint=endpoint,
        peer_policy=policy,
        provider_service=provider,
    )
    read_observations: list[str] = []

    def reject_after_read(_connection: socket.socket, *, deadline: float) -> bytes:
        del deadline
        read_observations.append("read")
        raise SourceBrokerTransportError("stop after read gate")

    monkeypatch.setattr(daemon_server_module, "read_frame_before_deadline", reject_after_read)
    left, right = socket.socketpair()
    monkeypatch.setattr(
        daemon_server_module,
        "_kernel_peer_credentials",
        lambda _connection: (
            123,
            runtime.identities.scheduler_client.uid,
            runtime.identities.scheduler_client.gid,
        ),
    )
    try:
        service._serve_connection(left)
    finally:
        right.close()
    assert read_observations == ["read"]

    read_observations.clear()
    left, right = socket.socketpair()
    monkeypatch.setattr(
        daemon_server_module,
        "_kernel_peer_credentials",
        lambda _connection: (
            124,
            runtime.identities.current_claim.uid,
            runtime.identities.current_claim.gid,
        ),
    )
    try:
        service._serve_connection(left)
    finally:
        right.close()
    assert read_observations == []


def test_production_aggregate_is_rejected_before_any_key_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    monkeypatch.setattr(
        authority_module,
        "_read_secure_key",
        lambda *_args, **_kwargs: pytest.fail("aggregate attempted to read a key"),
    )

    with pytest.raises(
        authority_module.SourceBrokerV2AuthorityCompositionError,
        match="aggregate.*forbidden|role-local",
    ):
        authority_module.compose_production_source_broker_v2_authorities(runtime)


def test_role_local_authority_service_module_and_factories_exist() -> None:
    assert importlib.util.find_spec("rquant.source_broker_v2_authority_service") is not None
    for name in (
        "compose_production_source_broker_v2_current_claim_service",
        "compose_production_source_broker_v2_source_quota_service",
        "compose_production_source_broker_v2_replay_lineage_service",
        "compose_production_source_broker_v2_scheduler_clients",
    ):
        assert hasattr(authority_module, name)


def test_authority_wire_is_strict_canonical_and_role_operation_bound() -> None:
    from rquant.source_broker_v2_authority_service import (
        SourceBrokerV2AuthorityOperation,
        SourceBrokerV2AuthorityPreflightPayload,
        SourceBrokerV2AuthorityServiceError,
        SourceBrokerV2AuthorityWireRequest,
        decode_authority_request,
        encode_authority_request,
    )

    request = SourceBrokerV2AuthorityWireRequest.from_payload(
        role=SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY,
        operation=SourceBrokerV2AuthorityOperation.PREFLIGHT,
        challenge="a" * 64,
        payload=SourceBrokerV2AuthorityPreflightPayload(),
    )
    encoded = encode_authority_request(request)
    assert decode_authority_request(encoded) == request

    noncanonical = json.dumps(
        json.loads(encoded),
        indent=2,
        sort_keys=False,
    ).encode("utf-8")
    with pytest.raises(SourceBrokerV2AuthorityServiceError, match="canonical"):
        decode_authority_request(noncanonical)

    wrong_role = request.model_copy(
        update={"role": SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY}
    )
    with pytest.raises(SourceBrokerV2AuthorityServiceError, match="role|operation"):
        encode_authority_request(wrong_role)


def test_role_local_factories_reject_another_role_before_filesystem_access(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = _runtime(tmp_path)
    current = runtime.identities.current_claim
    monkeypatch.setattr(authority_module.os, "geteuid", lambda: current.uid)
    monkeypatch.setattr(authority_module.os, "getegid", lambda: current.gid)
    monkeypatch.setattr(
        type(runtime),
        "validate_authority_filesystem",
        lambda *_args, **_kwargs: pytest.fail("wrong role reached filesystem validation"),
        raising=False,
    )

    with pytest.raises(
        authority_module.SourceBrokerV2AuthorityCompositionError,
        match="process identity|role identity",
    ):
        authority_module.compose_production_source_broker_v2_source_quota_service(runtime)

    with pytest.raises(
        authority_module.SourceBrokerV2AuthorityCompositionError,
        match="process identity|role identity",
    ):
        authority_module.compose_production_source_broker_v2_root_service(
            runtime,
            role=authority_module.SourceBrokerV2RootRole.CURRENT_CLAIM,
        )


def test_authority_service_accepts_only_exact_role_authority_and_canonical_request(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.source_broker_v2_authority_service import (
        SourceBrokerV2AuthorityAck,
        SourceBrokerV2AuthorityOperation,
        SourceBrokerV2AuthorityPreflightPayload,
        SourceBrokerV2AuthorityServiceError,
        SourceBrokerV2AuthorityUnixService,
        SourceBrokerV2AuthorityWireRequest,
        decode_authority_result,
    )

    runtime = _runtime(tmp_path)
    identity = runtime.identities.source_quota
    authority = object.__new__(SourceQuotaParentAuthority)
    monkeypatch.setattr(SourceQuotaParentAuthority, "audit", lambda _self: None)
    service = SourceBrokerV2AuthorityUnixService(
        endpoint=SocketEndpointPolicy(
            path=tmp_path / "quota.sock",
            owner_uid=identity.uid,
            group_gid=runtime.identities.scheduler_client.gid,
            mode=0o660,
        ),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({runtime.identities.scheduler_client.uid}),
            allowed_gids=frozenset({runtime.identities.scheduler_client.gid}),
        ),
        service_identity=identity,
        authority=authority,
    )
    request = SourceBrokerV2AuthorityWireRequest.from_payload(
        role=identity.role,
        operation=SourceBrokerV2AuthorityOperation.PREFLIGHT,
        challenge="b" * 64,
        payload=SourceBrokerV2AuthorityPreflightPayload(),
    )
    response = service.handle_request(request)
    ack = decode_authority_result(
        SourceBrokerV2AuthorityAck,
        response,
    )
    assert ack.accepted is True

    wrong_role = request.model_copy(
        update={"role": SourceBrokerV2ProcessRole.CURRENT_CLAIM_AUTHORITY}
    )
    with pytest.raises(SourceBrokerV2AuthorityServiceError, match="role"):
        service.handle_request(wrong_role)

    with pytest.raises(TypeError, match="exact role authority"):
        SourceBrokerV2AuthorityUnixService(
            endpoint=service.endpoint,
            peer_policy=service.peer_policy,
            service_identity=identity,
            authority=object(),
        )


def test_authority_service_rejects_peer_before_read_and_enforces_frame_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_authority_service as service_module
    from rquant.source_broker_v2_authority_service import (
        SOURCE_BROKER_V2_AUTHORITY_MAX_FRAME_BYTES,
        SourceBrokerV2AuthorityServiceError,
        SourceBrokerV2AuthorityUnixService,
        decode_authority_request,
    )

    runtime = _runtime(tmp_path)
    identity = runtime.identities.source_quota
    authority = object.__new__(SourceQuotaParentAuthority)
    service = SourceBrokerV2AuthorityUnixService(
        endpoint=SocketEndpointPolicy(
            path=tmp_path / "quota.sock",
            owner_uid=identity.uid,
            group_gid=runtime.identities.scheduler_client.gid,
            mode=0o660,
        ),
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({runtime.identities.scheduler_client.uid}),
            allowed_gids=frozenset({runtime.identities.scheduler_client.gid}),
        ),
        service_identity=identity,
        authority=authority,
    )
    monkeypatch.setattr(
        service_module,
        "_kernel_peer_credentials",
        lambda _connection: (
            123,
            runtime.identities.current_claim.uid,
            runtime.identities.current_claim.gid,
        ),
    )
    monkeypatch.setattr(
        service_module,
        "_read_frame_before_deadline",
        lambda *_args, **_kwargs: pytest.fail("authority read before peer gate"),
    )
    left, right = socket.socketpair()
    try:
        service._serve_connection(left)
    finally:
        left.close()
        right.close()

    with pytest.raises(SourceBrokerV2AuthorityServiceError, match="wire bound"):
        decode_authority_request(b"x" * (SOURCE_BROKER_V2_AUTHORITY_MAX_FRAME_BYTES + 1))


def test_authority_unix_service_client_roundtrip_uses_event_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.source_broker_v2_authority_service as service_module
    from rquant.source_broker_protocol import ServerCredentialsPolicy
    from rquant.source_broker_v2_authority_service import (
        SourceBrokerV2AuthorityAck,
        SourceBrokerV2AuthorityUnixService,
        SourceBrokerV2SourceQuotaUnixClient,
    )

    identity = _identity(
        SourceBrokerV2ProcessRole.SOURCE_QUOTA_AUTHORITY,
        uid=os.geteuid(),
        gid=os.getegid(),
    )
    authority = object.__new__(SourceQuotaParentAuthority)
    monkeypatch.setattr(SourceQuotaParentAuthority, "audit", lambda _self: None)
    monkeypatch.setattr(
        service_module,
        "_kernel_peer_credentials",
        lambda _connection: (os.getpid(), os.geteuid(), os.getegid()),
    )
    monkeypatch.setattr(
        service_module,
        "verify_connected_server_authority",
        lambda **_kwargs: None,
    )
    run_directory = Path.cwd() / f".rq-a-{os.getpid()}-{tmp_path.name[-5:]}"
    run_directory.mkdir(mode=0o700)
    endpoint = SocketEndpointPolicy(
        path=run_directory / "q.sock",
        owner_uid=identity.uid,
        group_gid=identity.gid,
        mode=0o660,
    )
    service = SourceBrokerV2AuthorityUnixService(
        endpoint=endpoint,
        peer_policy=PeerCredentialsPolicy(
            allowed_uids=frozenset({os.geteuid()}),
            allowed_gids=frozenset({os.getegid()}),
        ),
        service_identity=identity,
        authority=authority,
    )
    client = SourceBrokerV2SourceQuotaUnixClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=identity.uid,
            expected_gid=identity.gid,
        ),
        timeout_ms=1_000,
    )
    thread = threading.Thread(target=service.serve_forever, name="quota-authority-test")
    thread.start()
    try:
        assert service.wait_ready(timeout=2.0)
        assert client.preflight() == SourceBrokerV2AuthorityAck()
    finally:
        service.shutdown()
        thread.join(timeout=2.0)
        run_directory.rmdir()
    assert not thread.is_alive()


class _AuthorityDeadlineClock:
    def __init__(self) -> None:
        self.value = 0.0
        self._script: list[float] = []

    def __call__(self) -> float:
        if self._script:
            self.value = self._script.pop(0)
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds

    def script(self, *values: float) -> None:
        self._script.extend(values)


class _AuthorityDeadlineSocket:
    def __enter__(self) -> _AuthorityDeadlineSocket:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def connect(self, _path: str) -> None:
        return None

    def settimeout(self, _timeout: float) -> None:
        return None


@pytest.mark.parametrize(
    "overrun_stage",
    ("canonical-decode", "binding", "typed-decode", "signature-verify", "final-return"),
)
def test_authority_client_total_deadline_rejects_correct_late_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrun_stage: str,
) -> None:
    import rquant.source_broker_v2_authority_service as service_module
    from rquant.source_broker_protocol import ServerCredentialsPolicy
    from rquant.source_broker_v2_authority_service import (
        SourceBrokerV2AuthorityAck,
        SourceBrokerV2AuthorityServiceError,
        SourceBrokerV2AuthorityWireResponse,
        SourceBrokerV2SourceQuotaUnixClient,
        decode_authority_request,
        encode_authority_response,
    )

    clock = _AuthorityDeadlineClock()
    captured: dict[str, bytes] = {}
    endpoint = SocketEndpointPolicy(
        path=tmp_path / "quota.sock",
        owner_uid=os.geteuid(),
        group_gid=os.getegid(),
        mode=0o660,
    )
    client = SourceBrokerV2SourceQuotaUnixClient(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        ),
        timeout_ms=100,
    )
    monkeypatch.setattr(service_module.time, "monotonic", clock)
    monkeypatch.setattr(service_module.socket, "socket", lambda *_args: _AuthorityDeadlineSocket())
    monkeypatch.setattr(service_module, "validate_socket_endpoint", lambda _policy: object())
    monkeypatch.setattr(
        service_module,
        "_kernel_peer_credentials",
        lambda _connection: (os.getpid(), os.geteuid(), os.getegid()),
    )
    monkeypatch.setattr(
        service_module,
        "verify_connected_server_authority",
        lambda **_kwargs: None,
    )

    def capture_write(_connection: object, payload: bytes, *, deadline: float) -> None:
        del deadline
        captured["request"] = payload

    def return_valid_response(_connection: object, *, deadline: float) -> bytes:
        del deadline
        request = decode_authority_request(captured["request"])
        return encode_authority_response(
            SourceBrokerV2AuthorityWireResponse.from_result(
                request=request,
                result=SourceBrokerV2AuthorityAck(),
            )
        )

    monkeypatch.setattr(service_module, "_write_frame_before_deadline", capture_write)
    monkeypatch.setattr(service_module, "_read_frame_before_deadline", return_valid_response)

    target_by_stage: dict[str, tuple[str, str]] = {
        "canonical-decode": ("module", "decode_authority_response"),
        "binding": ("module", "_require_response_binding"),
        "typed-decode": ("module", "decode_authority_result"),
        "signature-verify": ("client", "_verify_typed_result_signature"),
    }
    if overrun_stage in target_by_stage:
        owner_name, attribute = target_by_stage[overrun_stage]
        owner: Any = service_module if owner_name == "module" else client
        original = getattr(owner, attribute)

        def cross_deadline(*args: object, **kwargs: object) -> Any:
            result = original(*args, **kwargs)
            clock.advance(0.101)
            return result

        monkeypatch.setattr(owner, attribute, cross_deadline)
    else:
        original = client._verify_typed_result_signature

        def expire_at_final_return(*args: object, **kwargs: object) -> Any:
            result = original(*args, **kwargs)
            clock.script(clock.value, 0.101)
            return result

        monkeypatch.setattr(client, "_verify_typed_result_signature", expire_at_final_return)

    with pytest.raises(SourceBrokerV2AuthorityServiceError, match="deadline"):
        client.preflight()


@pytest.mark.parametrize(
    "client_name",
    ("current_claim", "source_quota", "replay_lineage"),
)
def test_authority_preflight_caps_its_timeout_at_the_caller_absolute_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    client_name: str,
) -> None:
    import rquant.source_broker_v2_authority_service as service_module
    from rquant.source_broker_protocol import ServerCredentialsPolicy
    from rquant.source_broker_v2_authority_service import (
        SourceBrokerV2AuthorityServiceError,
        SourceBrokerV2CurrentClaimUnixClient,
        SourceBrokerV2ReplayLineageUnixClient,
        SourceBrokerV2SourceQuotaUnixClient,
    )

    clock = _AuthorityDeadlineClock()
    endpoint = SocketEndpointPolicy(
        path=tmp_path / f"{client_name}.sock",
        owner_uid=os.geteuid(),
        group_gid=os.getegid(),
        mode=0o660,
    )
    client_type = {
        "current_claim": SourceBrokerV2CurrentClaimUnixClient,
        "source_quota": SourceBrokerV2SourceQuotaUnixClient,
        "replay_lineage": SourceBrokerV2ReplayLineageUnixClient,
    }[client_name]
    client = client_type(
        endpoint=endpoint,
        server_policy=ServerCredentialsPolicy(
            expected_uid=os.geteuid(),
            expected_gid=os.getegid(),
        ),
        timeout_ms=2_000,
    )
    observed_deadlines: list[float] = []
    monkeypatch.setattr(service_module.time, "monotonic", clock)
    monkeypatch.setattr(service_module.socket, "socket", lambda *_args: _AuthorityDeadlineSocket())
    monkeypatch.setattr(service_module, "validate_socket_endpoint", lambda _policy: object())
    monkeypatch.setattr(
        service_module,
        "_kernel_peer_credentials",
        lambda _connection: (os.getpid(), os.geteuid(), os.getegid()),
    )
    monkeypatch.setattr(
        service_module,
        "verify_connected_server_authority",
        lambda **_kwargs: None,
    )

    def fail_write(_connection: object, _payload: bytes, *, deadline: float) -> None:
        observed_deadlines.append(deadline)
        raise SourceBrokerTransportError("stop after deadline capture")

    monkeypatch.setattr(service_module, "_write_frame_before_deadline", fail_write)
    caller_deadline = 0.05
    with pytest.raises(SourceBrokerV2AuthorityServiceError, match="unavailable|deadline"):
        client.preflight(deadline=caller_deadline)
    assert observed_deadlines == [caller_deadline]
