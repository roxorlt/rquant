"""Authenticated Linux Unix client for the closed Source Broker protocol."""

from __future__ import annotations

import socket
import struct
import time

from rquant.source_broker_protocol import (
    ServerCredentialsPolicy,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
    SourceBrokerTransportFailure,
    SourceBrokerTransportRemoteError,
    SourceBrokerTransportRequest,
    SourceBrokerTransportResponse,
    decode_response,
    encode_request,
    read_frame,
    require_linux_source_broker_transport,
    validate_response_binding,
    validate_socket_endpoint,
    verify_connected_server_authority,
    write_frame,
)


class SourceBrokerUnixClient:
    """One-shot client; replay state remains entirely inside SourceBroker."""

    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        server_policy: ServerCredentialsPolicy,
        timeout_seconds: float,
        max_attempts: int = 2,
    ) -> None:
        if not 0 < timeout_seconds <= 30:
            raise ValueError("source broker client timeout is invalid")
        if not 1 <= max_attempts <= 5:
            raise ValueError("source broker client retry budget is invalid")
        self._endpoint = endpoint
        self._server_policy = server_policy
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts

    def execute(self, request: SourceBrokerTransportRequest) -> SourceBrokerTransportResponse:
        require_linux_source_broker_transport()
        payload = encode_request(request)
        last_error: BaseException | None = None
        for attempt in range(self._max_attempts):
            try:
                endpoint_identity = validate_socket_endpoint(self._endpoint)
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                    connection.settimeout(self._timeout_seconds)
                    connection.connect(str(self._endpoint.path))
                    server_pid, server_uid, server_gid = _kernel_peer_credentials(connection)
                    if not self._server_policy.allows(
                        pid=server_pid,
                        uid=server_uid,
                        gid=server_gid,
                    ):
                        raise SourceBrokerTransportError(
                            "connected Source Broker server credentials are not allowed"
                        )
                    verify_connected_server_authority(
                        server_pid=server_pid,
                        endpoint=self._endpoint,
                        endpoint_identity=endpoint_identity,
                    )
                    validate_socket_endpoint(
                        self._endpoint,
                        expected_identity=endpoint_identity,
                    )
                    write_frame(connection, payload)
                    wire_response = decode_response(read_frame(connection))
                if isinstance(wire_response, SourceBrokerTransportFailure):
                    raise SourceBrokerTransportRemoteError(wire_response.error)
                validate_response_binding(request=request, response=wire_response)
                return wire_response
            except SourceBrokerTransportRemoteError:
                raise
            except (OSError, TimeoutError, ValueError, SourceBrokerTransportError) as exc:
                last_error = exc
                if attempt + 1 < self._max_attempts:
                    time.sleep(min(0.05 * (2**attempt), 0.25))
        raise SourceBrokerTransportError("source broker Unix transport failed") from last_error


class SourceBrokerClient:
    def __init__(self, transport: SourceBrokerUnixClient) -> None:
        self._transport = transport

    def execute(self, request: SourceBrokerTransportRequest) -> SourceBrokerTransportResponse:
        return self._transport.execute(request)


def build_source_broker_client(
    *,
    offline: bool,
    endpoint: SocketEndpointPolicy,
    server_policy: ServerCredentialsPolicy,
    timeout_seconds: float,
    max_attempts: int = 2,
) -> SourceBrokerClient | None:
    if type(offline) is not bool:
        raise ValueError("source broker offline mode is invalid")
    if offline:
        return None
    return SourceBrokerClient(
        SourceBrokerUnixClient(
            endpoint=endpoint,
            server_policy=server_policy,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )
    )


def _kernel_peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
    """Read the connected server identity directly from Linux ``SO_PEERCRED``."""

    try:
        raw = connection.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_PEERCRED,
            struct.calcsize("3i"),
        )
        pid, uid, gid = struct.unpack("3i", raw)
    except (OSError, struct.error) as exc:
        raise SourceBrokerTransportError(
            "connected Source Broker credentials are unavailable"
        ) from exc
    if pid <= 0 or uid < 0 or gid < 0:
        raise SourceBrokerTransportError("connected Source Broker credentials are invalid")
    return pid, uid, gid
