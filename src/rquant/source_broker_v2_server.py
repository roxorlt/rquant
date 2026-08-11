"""Secure AF_UNIX daemon for the SourceBroker v2 provider service."""

from __future__ import annotations

import errno
import os
import socket
import stat
import struct
import time
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from threading import Event

from rquant.source_broker_protocol import (
    MAX_SOURCE_BROKER_FRAME_BYTES,
    PeerCredentialsPolicy,
    SocketEndpointIdentity,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
    source_broker_peer_credentials_supported,
    validate_socket_parent,
)
from rquant.source_broker_v2 import SourceBrokerV2TransportDeadlineError
from rquant.source_broker_v2_service import (
    DecodedV2WireRequest,
    SourceBrokerV2ProviderService,
    decode_v2_wire_request,
)

_FRAME_HEADER_BYTES = 4


@dataclass(frozen=True)
class _AncestorAuthority:
    path: str
    device: int
    inode: int
    owner_uid: int
    group_gid: int
    mode: int


@dataclass(frozen=True)
class _DirectoryAuthority:
    fd: int
    ancestors: tuple[_AncestorAuthority, ...]
    device: int
    inode: int
    owner_uid: int
    group_gid: int
    mode: int


@dataclass(frozen=True)
class _ListenerAuthority:
    directory: _DirectoryAuthority
    endpoint: SocketEndpointIdentity
    listener_device: int
    listener_inode: int


class SourceBrokerV2UnixService:
    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        peer_policy: PeerCredentialsPolicy,
        provider_service: SourceBrokerV2ProviderService,
        total_request_deadline_seconds: float = 10.0,
        accept_timeout_seconds: float = 0.1,
        backlog: int = 32,
    ) -> None:
        if type(endpoint) is not SocketEndpointPolicy:
            raise TypeError("SourceBroker v2 endpoint policy must be exact")
        if type(peer_policy) is not PeerCredentialsPolicy:
            raise TypeError("SourceBroker v2 peer policy must be exact")
        if type(provider_service) is not SourceBrokerV2ProviderService:
            raise TypeError("SourceBroker v2 provider service must be exact")
        if not 0 < total_request_deadline_seconds <= 30:
            raise ValueError("SourceBroker v2 request deadline is invalid")
        if not 0 < accept_timeout_seconds <= 5:
            raise ValueError("SourceBroker v2 accept timeout is invalid")
        if type(backlog) is not int or not 1 <= backlog <= 256:
            raise ValueError("SourceBroker v2 backlog is invalid")
        self._endpoint = endpoint
        self._peer_policy = peer_policy
        self._provider_service = provider_service
        self._total_request_deadline_seconds = total_request_deadline_seconds
        self._accept_timeout_seconds = accept_timeout_seconds
        self._backlog = backlog
        self._stopping = Event()
        self.ready = Event()

    def serve_forever(
        self,
        *,
        stop: Event | None = None,
        max_connections: int | None = None,
    ) -> None:
        _require_linux_v2_server()
        if max_connections is not None and max_connections < 1:
            raise ValueError("SourceBroker v2 max_connections is invalid")
        served = 0
        listener, authority = self._bind_listener()
        try:
            with listener:
                listener.settimeout(self._accept_timeout_seconds)
                self._provider_service.reconcile_abandoned_invocations_after_listener_acquired()
                self.ready.set()
                while max_connections is None or served < max_connections:
                    if self._stopping.is_set() or stop is not None and stop.is_set():
                        break
                    _validate_listener_authority(listener, authority, self._endpoint)
                    try:
                        connection, _address = listener.accept()
                    except TimeoutError:
                        continue
                    served += 1
                    try:
                        _validate_listener_authority(listener, authority, self._endpoint)
                    except BaseException:
                        connection.close()
                        raise
                    self._serve_connection(connection)
        finally:
            self.ready.clear()
            self._cleanup_listener_authority(authority)

    def wake(self) -> None:
        if not self._endpoint.path.exists():
            return
        with suppress(OSError), socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.settimeout(0.2)
            connection.connect(str(self._endpoint.path))

    def stop(self) -> None:
        self._stopping.set()
        self._provider_service.stop()
        self.wake()

    def _bind_listener(self) -> tuple[socket.socket, _ListenerAuthority]:
        _require_linux_v2_server()
        if os.geteuid() != self._endpoint.owner_uid:
            raise SourceBrokerTransportError(
                "SourceBroker v2 service uid does not own the socket endpoint"
            )
        directory = _open_directory_authority(self._endpoint)
        listener: socket.socket | None = None
        try:
            _unlink_stale_endpoint(directory, self._endpoint)
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(str(self._endpoint.path))
            os.chmod(
                self._endpoint.path.name,
                self._endpoint.mode,
                dir_fd=directory.fd,
            )
            os.chown(
                self._endpoint.path.name,
                self._endpoint.owner_uid,
                self._endpoint.group_gid,
                dir_fd=directory.fd,
                follow_symlinks=False,
            )
            listener.listen(self._backlog)
            endpoint_identity = _endpoint_identity_at(directory, self._endpoint)
            opened = os.fstat(listener.fileno())
            if not stat.S_ISSOCK(opened.st_mode):
                raise SourceBrokerTransportError("SourceBroker v2 listener fd is not a socket")
            authority = _ListenerAuthority(
                directory=directory,
                endpoint=endpoint_identity,
                listener_device=opened.st_dev,
                listener_inode=opened.st_ino,
            )
            _validate_listener_authority(listener, authority, self._endpoint)
            return listener, authority
        except BaseException:
            if listener is not None:
                listener.close()
            os.close(directory.fd)
            raise

    def _cleanup_listener_authority(self, authority: _ListenerAuthority) -> None:
        try:
            _validate_directory_authority(authority.directory, self._endpoint)
            current = _endpoint_identity_at(authority.directory, self._endpoint)
            if current == authority.endpoint:
                os.unlink(self._endpoint.path.name, dir_fd=authority.directory.fd)
        except (OSError, SourceBrokerTransportError):
            pass
        finally:
            os.close(authority.directory.fd)

    def _serve_connection(self, connection: socket.socket) -> None:
        deadline = time.monotonic() + self._total_request_deadline_seconds
        decoded: DecodedV2WireRequest | None = None
        peer_rejected = False
        with connection:
            try:
                _require_deadline(deadline, stage="before peer credentials")
                pid, uid, gid = _kernel_peer_credentials(connection)
                if not self._peer_policy.allows(pid=pid, uid=uid, gid=gid):
                    error = SourceBrokerTransportError("V2 source peer credentials are not allowed")
                    peer_rejected = True
                    self._provider_service.record_transport_event(
                        category="peer_rejected",
                        error=error,
                    )
                    raise error
                _require_deadline(deadline, stage="after peer credentials")
                payload = read_frame_before_deadline(connection, deadline=deadline)
                _require_deadline(deadline, stage="after request read")
                decoded = decode_v2_wire_request(payload)
                response = self._provider_service.handle_wire_request(
                    payload,
                    deadline=deadline,
                )
            except SourceBrokerTransportError as exc:
                if not peer_rejected:
                    self._provider_service.record_transport_event(
                        category="transport_error",
                        error=exc,
                        operation_id=_decoded_operation_id(decoded),
                    )
                if decoded is not None:
                    self._write_bound_failure(connection, decoded, str(exc), deadline=deadline)
            except Exception as exc:
                self._provider_service.record_transport_event(
                    category="transport_error",
                    error=exc,
                    operation_id=_decoded_operation_id(decoded),
                )
                if decoded is not None:
                    self._write_bound_failure(
                        connection,
                        decoded,
                        "SourceBroker v2 provider service failed",
                        deadline=deadline,
                    )
            else:
                try:
                    _require_deadline(deadline, stage="after request processing")
                    write_frame_before_deadline(connection, response, deadline=deadline)
                except Exception as exc:
                    self._provider_service.record_transport_event(
                        category="write_error",
                        error=exc,
                        operation_id=_decoded_operation_id(decoded),
                    )

    def _write_bound_failure(
        self,
        connection: socket.socket,
        decoded: DecodedV2WireRequest,
        error: str,
        *,
        deadline: float,
    ) -> None:
        try:
            failure = self._provider_service.handle_wire_failure(decoded.wire, error)
            write_frame_before_deadline(connection, failure, deadline=deadline)
        except SourceBrokerTransportError as exc:
            self._provider_service.record_transport_event(
                category="write_error",
                error=exc,
                operation_id=_decoded_operation_id(decoded),
            )


def _decoded_operation_id(decoded: DecodedV2WireRequest | None) -> str | None:
    if decoded is None:
        return None
    with suppress(Exception):
        payload = decoded.parse_payload()
        request = getattr(payload, "request", payload)
        operation_id = getattr(request, "operation_id", None)
        if type(operation_id) is str:
            return operation_id
    return None


def read_frame_before_deadline(connection: socket.socket, *, deadline: float) -> bytes:
    header = _recv_exact_before_deadline(
        connection,
        _FRAME_HEADER_BYTES,
        deadline=deadline,
    )
    size = int.from_bytes(header, "big", signed=False)
    if not 0 < size <= MAX_SOURCE_BROKER_FRAME_BYTES:
        raise SourceBrokerTransportError("SourceBroker v2 frame size is invalid")
    return _recv_exact_before_deadline(connection, size, deadline=deadline)


def write_frame_before_deadline(
    connection: socket.socket,
    payload: bytes,
    *,
    deadline: float,
) -> None:
    if type(payload) is not bytes or not 0 < len(payload) <= MAX_SOURCE_BROKER_FRAME_BYTES:
        raise SourceBrokerTransportError("SourceBroker v2 frame size is invalid")
    frame = memoryview(len(payload).to_bytes(_FRAME_HEADER_BYTES, "big") + payload)
    while frame:
        remaining = _remaining(deadline, stage="before frame write")
        connection.settimeout(remaining)
        try:
            sent = connection.send(frame)
        except OSError as exc:
            raise SourceBrokerTransportError("SourceBroker v2 frame write failed") from exc
        if type(sent) is not int or sent <= 0 or sent > len(frame):
            raise SourceBrokerTransportError("SourceBroker v2 frame write made no progress")
        frame = frame[sent:]
        _require_deadline(deadline, stage="after frame write")


def _recv_exact_before_deadline(
    connection: socket.socket,
    size: int,
    *,
    deadline: float,
) -> bytes:
    chunks: list[bytes] = []
    remaining_bytes = size
    while remaining_bytes:
        remaining = _remaining(deadline, stage="before frame read")
        connection.settimeout(remaining)
        try:
            chunk = connection.recv(remaining_bytes)
        except OSError as exc:
            raise SourceBrokerTransportError("SourceBroker v2 frame read failed") from exc
        if type(chunk) is not bytes or not chunk or len(chunk) > remaining_bytes:
            raise SourceBrokerTransportError("SourceBroker v2 frame is truncated")
        chunks.append(chunk)
        remaining_bytes -= len(chunk)
        _require_deadline(deadline, stage="after frame read")
    return b"".join(chunks)


def _kernel_peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
    try:
        raw = connection.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_PEERCRED,
            struct.calcsize("3i"),
        )
        pid, uid, gid = struct.unpack("3i", raw)
    except (OSError, struct.error) as exc:
        raise SourceBrokerTransportError("V2 source peer credentials are unavailable") from exc
    if pid <= 0 or uid < 0 or gid < 0:
        raise SourceBrokerTransportError("V2 source peer credentials are invalid")
    return pid, uid, gid


def _open_directory_authority(endpoint: SocketEndpointPolicy) -> _DirectoryAuthority:
    validate_socket_parent(endpoint)
    ancestors = _pin_ancestor_authority(endpoint.path.parent)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(endpoint.path.parent, flags)
    except OSError as exc:
        raise SourceBrokerTransportError("SourceBroker v2 socket directory is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        named = os.lstat(endpoint.path.parent)
        mode = stat.S_IMODE(opened.st_mode)
        pinned_parent = ancestors[-1]
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(named.st_mode)
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or (opened.st_dev, opened.st_ino) != (pinned_parent.device, pinned_parent.inode)
            or (opened.st_uid, opened.st_gid, mode)
            != (named.st_uid, named.st_gid, stat.S_IMODE(named.st_mode))
            or opened.st_uid not in {0, endpoint.owner_uid}
            or mode & 0o022
        ):
            raise SourceBrokerTransportError("SourceBroker v2 socket directory is unsafe")
        authority = _DirectoryAuthority(
            fd=descriptor,
            ancestors=ancestors,
            device=opened.st_dev,
            inode=opened.st_ino,
            owner_uid=opened.st_uid,
            group_gid=opened.st_gid,
            mode=mode,
        )
        _validate_directory_authority(authority, endpoint)
        return authority
    except BaseException:
        os.close(descriptor)
        raise


def _validate_listener_authority(
    listener: socket.socket,
    authority: _ListenerAuthority,
    endpoint: SocketEndpointPolicy,
) -> None:
    _validate_directory_authority(authority.directory, endpoint)
    current_endpoint = _endpoint_identity_at(authority.directory, endpoint)
    if current_endpoint != authority.endpoint:
        raise SourceBrokerTransportError("SourceBroker v2 endpoint authority was replaced")
    opened_listener = os.fstat(listener.fileno())
    if (
        not stat.S_ISSOCK(opened_listener.st_mode)
        or (opened_listener.st_dev, opened_listener.st_ino)
        != (authority.listener_device, authority.listener_inode)
        or listener.family != socket.AF_UNIX
        or listener.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM
        or listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN) != 1
        or listener.getsockname() != str(endpoint.path)
    ):
        raise SourceBrokerTransportError("SourceBroker v2 listener authority is invalid")


def _endpoint_identity_at(
    directory: _DirectoryAuthority,
    endpoint: SocketEndpointPolicy,
) -> SocketEndpointIdentity:
    try:
        observed = os.stat(
            endpoint.path.name,
            dir_fd=directory.fd,
            follow_symlinks=False,
        )
    except OSError as exc:
        raise SourceBrokerTransportError(
            "SourceBroker v2 endpoint authority is unavailable"
        ) from exc
    if stat.S_ISLNK(observed.st_mode):
        raise SourceBrokerTransportError("SourceBroker v2 endpoint authority is a symlink")
    if not stat.S_ISSOCK(observed.st_mode):
        raise SourceBrokerTransportError("SourceBroker v2 endpoint authority is not a socket")
    mode = stat.S_IMODE(observed.st_mode)
    identity = SocketEndpointIdentity(
        device=observed.st_dev,
        inode=observed.st_ino,
        owner_uid=observed.st_uid,
        group_gid=observed.st_gid,
        mode=mode,
    )
    if (identity.owner_uid, identity.group_gid, identity.mode) != (
        endpoint.owner_uid,
        endpoint.group_gid,
        endpoint.mode,
    ):
        raise SourceBrokerTransportError("SourceBroker v2 endpoint metadata is invalid")
    return identity


def _unlink_stale_endpoint(directory: _DirectoryAuthority, endpoint: SocketEndpointPolicy) -> None:
    _validate_directory_authority(directory, endpoint)
    try:
        initial = _endpoint_identity_at(directory, endpoint)
    except SourceBrokerTransportError:
        try:
            os.stat(endpoint.path.name, dir_fd=directory.fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        except OSError as stat_exc:
            raise SourceBrokerTransportError(
                "SourceBroker v2 stale endpoint is unavailable"
            ) from stat_exc
        raise
    if _endpoint_accepts_connection(endpoint):
        raise SourceBrokerTransportError("SourceBroker v2 endpoint is live; refusing to unlink")
    _validate_directory_authority(directory, endpoint)
    current = _endpoint_identity_at(directory, endpoint)
    if current != initial:
        raise SourceBrokerTransportError("SourceBroker v2 stale endpoint identity changed")
    os.unlink(endpoint.path.name, dir_fd=directory.fd)


def _pin_ancestor_authority(parent: Path) -> tuple[_AncestorAuthority, ...]:
    paths = tuple(reversed((parent, *parent.parents)))
    pinned: list[_AncestorAuthority] = []
    for path in paths:
        try:
            observed = os.lstat(path)
        except OSError as exc:
            raise SourceBrokerTransportError(
                "SourceBroker v2 socket ancestor is unavailable"
            ) from exc
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise SourceBrokerTransportError(
                "SourceBroker v2 socket ancestor must be a real directory"
            )
        pinned.append(
            _AncestorAuthority(
                path=os.fspath(path),
                device=observed.st_dev,
                inode=observed.st_ino,
                owner_uid=observed.st_uid,
                group_gid=observed.st_gid,
                mode=stat.S_IMODE(observed.st_mode),
            )
        )
    return tuple(pinned)


def _validate_directory_authority(
    directory: _DirectoryAuthority,
    endpoint: SocketEndpointPolicy,
) -> None:
    if not directory.ancestors or directory.ancestors[-1].path != os.fspath(endpoint.path.parent):
        raise SourceBrokerTransportError("SourceBroker v2 directory authority path changed")
    for expected in directory.ancestors:
        try:
            observed = os.lstat(expected.path)
        except OSError as exc:
            raise SourceBrokerTransportError(
                "SourceBroker v2 socket ancestor authority is unavailable"
            ) from exc
        current = (
            observed.st_dev,
            observed.st_ino,
            observed.st_uid,
            observed.st_gid,
            stat.S_IMODE(observed.st_mode),
        )
        pinned = (
            expected.device,
            expected.inode,
            expected.owner_uid,
            expected.group_gid,
            expected.mode,
        )
        if (
            stat.S_ISLNK(observed.st_mode)
            or not stat.S_ISDIR(observed.st_mode)
            or current != pinned
        ):
            raise SourceBrokerTransportError("SourceBroker v2 socket ancestor authority changed")
    opened = os.fstat(directory.fd)
    opened_identity = (
        opened.st_dev,
        opened.st_ino,
        opened.st_uid,
        opened.st_gid,
        stat.S_IMODE(opened.st_mode),
    )
    expected_parent = (
        directory.device,
        directory.inode,
        directory.owner_uid,
        directory.group_gid,
        directory.mode,
    )
    if not stat.S_ISDIR(opened.st_mode) or opened_identity != expected_parent:
        raise SourceBrokerTransportError("SourceBroker v2 directory authority changed")


def _endpoint_accepts_connection(endpoint: SocketEndpointPolicy) -> bool:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(0.2)
        try:
            probe.connect(str(endpoint.path))
        except TimeoutError as exc:
            raise SourceBrokerTransportError(
                "SourceBroker v2 stale endpoint probe timed out"
            ) from exc
        except OSError as exc:
            if exc.errno == errno.ECONNREFUSED:
                return False
            raise SourceBrokerTransportError(
                "SourceBroker v2 stale endpoint could not be proven stale"
            ) from exc
        return True


def _require_linux_v2_server() -> None:
    if not source_broker_peer_credentials_supported():
        raise SourceBrokerTransportError(
            "Linux SO_PEERCRED is required for SourceBroker v2 server transport"
        )


def _remaining(deadline: float, *, stage: str) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise SourceBrokerV2TransportDeadlineError(
            f"V2 source broker server deadline expired {stage}"
        )
    return remaining


def _require_deadline(deadline: float, *, stage: str) -> None:
    _remaining(deadline, stage=stage)
