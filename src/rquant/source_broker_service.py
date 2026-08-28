"""Secure Linux Unix service for the closed Source Broker transport adapter."""

from __future__ import annotations

import os
import socket
import stat
import struct
from dataclasses import dataclass

from rquant.source_broker_protocol import (
    PeerCredentialsPolicy,
    SocketEndpointIdentity,
    SocketEndpointPolicy,
    SourceBrokerTransportError,
    decode_request,
    encode_failure,
    encode_response,
    read_frame,
    require_linux_source_broker_transport,
    validate_response_binding,
    validate_socket_parent,
    write_frame,
)
from rquant.source_broker_transport_adapter import SourceBrokerTransportAdapter


@dataclass(frozen=True)
class _DirectoryAuthority:
    fd: int
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

    @property
    def directory_fd(self) -> int:
        return self.directory.fd


class SourceBrokerUnixService:
    def __init__(
        self,
        *,
        endpoint: SocketEndpointPolicy,
        peer_policy: PeerCredentialsPolicy,
        adapter: SourceBrokerTransportAdapter,
        connection_timeout_seconds: float = 10.0,
    ) -> None:
        if not isinstance(adapter, SourceBrokerTransportAdapter):
            raise ValueError("source broker transport adapter is invalid")
        if not 0 < connection_timeout_seconds <= 30:
            raise ValueError("source broker connection timeout is invalid")
        self._endpoint = endpoint
        self._peer_policy = peer_policy
        self._adapter = adapter
        self._connection_timeout_seconds = connection_timeout_seconds

    def serve(self, *, max_connections: int | None = None) -> None:
        require_linux_source_broker_transport()
        if max_connections is not None and max_connections < 1:
            raise ValueError("source broker maximum connections is invalid")
        listener, authority = self._bind_listener()
        served = 0
        try:
            with listener:
                while max_connections is None or served < max_connections:
                    _validate_listener_authority(listener, authority, self._endpoint)
                    connection, _address = listener.accept()
                    served += 1
                    try:
                        _validate_listener_authority(listener, authority, self._endpoint)
                    except Exception:
                        connection.close()
                        raise
                    self._serve_connection(connection)
        finally:
            self._cleanup_listener_authority(authority)

    def _bind_listener(self) -> tuple[socket.socket, _ListenerAuthority]:
        require_linux_source_broker_transport()
        if os.geteuid() != self._endpoint.owner_uid:
            raise SourceBrokerTransportError(
                "source broker service uid does not own the socket endpoint"
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
            listener.listen(32)
            endpoint_identity = _endpoint_identity_at(directory, self._endpoint)
            opened = os.fstat(listener.fileno())
            if not stat.S_ISSOCK(opened.st_mode):
                raise SourceBrokerTransportError("source broker listener fd is not a socket")
            authority = _ListenerAuthority(
                directory=directory,
                endpoint=endpoint_identity,
                listener_device=opened.st_dev,
                listener_inode=opened.st_ino,
            )
            _validate_listener_authority(listener, authority, self._endpoint)
            return listener, authority
        except Exception:
            if listener is not None:
                listener.close()
            os.close(directory.fd)
            raise

    def _cleanup_listener_authority(self, authority: _ListenerAuthority) -> None:
        try:
            current = _endpoint_identity_at(authority.directory, self._endpoint)
            if current == authority.endpoint:
                os.unlink(self._endpoint.path.name, dir_fd=authority.directory.fd)
        except (OSError, SourceBrokerTransportError):
            pass
        finally:
            os.close(authority.directory.fd)

    def _serve_connection(self, connection: socket.socket) -> None:
        with connection:
            connection.settimeout(self._connection_timeout_seconds)
            try:
                pid, uid, gid = _kernel_peer_credentials(connection)
                if not self._peer_policy.allows(pid=pid, uid=uid, gid=gid):
                    raise SourceBrokerTransportError("peer credentials are not allowed")
                request = decode_request(read_frame(connection))
                response = self._adapter.handle(request)
                validate_response_binding(request=request, response=response)
                write_frame(connection, encode_response(response))
            except SourceBrokerTransportError as exc:
                self._write_failure(connection, _safe_transport_error(exc))
            except Exception:  # noqa: BLE001
                self._write_failure(connection, "source broker application failed")

    @staticmethod
    def _write_failure(connection: socket.socket, error: str) -> None:
        try:
            write_frame(connection, encode_failure(error))
        except SourceBrokerTransportError:
            return


def _kernel_peer_credentials(connection: socket.socket) -> tuple[int, int, int]:
    """Read Linux ``SO_PEERCRED`` before any request frame bytes are consumed."""

    try:
        raw = connection.getsockopt(
            socket.SOL_SOCKET,
            socket.SO_PEERCRED,
            struct.calcsize("3i"),
        )
        pid, uid, gid = struct.unpack("3i", raw)
    except (OSError, struct.error) as exc:
        raise SourceBrokerTransportError("source broker peer credentials are unavailable") from exc
    if pid <= 0 or uid < 0 or gid < 0:
        raise SourceBrokerTransportError("source broker peer credentials are invalid")
    return pid, uid, gid


def _open_directory_authority(endpoint: SocketEndpointPolicy) -> _DirectoryAuthority:
    validate_socket_parent(endpoint)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(endpoint.path.parent, flags)
    except OSError as exc:
        raise SourceBrokerTransportError("source broker socket directory is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        named = os.stat(endpoint.path.parent, follow_symlinks=False)
        mode = stat.S_IMODE(opened.st_mode)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or not stat.S_ISDIR(named.st_mode)
            or (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or (opened.st_uid, opened.st_gid, mode)
            != (named.st_uid, named.st_gid, stat.S_IMODE(named.st_mode))
            or opened.st_uid not in {0, endpoint.owner_uid}
            or mode & 0o022
        ):
            raise SourceBrokerTransportError("source broker socket directory is unsafe")
        return _DirectoryAuthority(
            fd=descriptor,
            device=opened.st_dev,
            inode=opened.st_ino,
            owner_uid=opened.st_uid,
            group_gid=opened.st_gid,
            mode=mode,
        )
    except Exception:
        os.close(descriptor)
        raise


def _validate_listener_authority(
    listener: socket.socket,
    authority: _ListenerAuthority,
    endpoint: SocketEndpointPolicy,
) -> None:
    directory = os.fstat(authority.directory.fd)
    named_directory = os.stat(endpoint.path.parent, follow_symlinks=False)
    expected_directory = (
        authority.directory.device,
        authority.directory.inode,
        authority.directory.owner_uid,
        authority.directory.group_gid,
        authority.directory.mode,
    )
    opened_directory = (
        directory.st_dev,
        directory.st_ino,
        directory.st_uid,
        directory.st_gid,
        stat.S_IMODE(directory.st_mode),
    )
    current_named_directory = (
        named_directory.st_dev,
        named_directory.st_ino,
        named_directory.st_uid,
        named_directory.st_gid,
        stat.S_IMODE(named_directory.st_mode),
    )
    if (
        not stat.S_ISDIR(directory.st_mode)
        or not stat.S_ISDIR(named_directory.st_mode)
        or opened_directory != expected_directory
        or current_named_directory != expected_directory
    ):
        raise SourceBrokerTransportError("source broker directory authority changed")
    current_endpoint = _endpoint_identity_at(authority.directory, endpoint)
    if current_endpoint != authority.endpoint:
        raise SourceBrokerTransportError("source broker endpoint authority was replaced")
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
        raise SourceBrokerTransportError("source broker listener authority is invalid")


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
        raise SourceBrokerTransportError("source broker endpoint authority is unavailable") from exc
    if stat.S_ISLNK(observed.st_mode):
        raise SourceBrokerTransportError("source broker endpoint authority is a symlink")
    if not stat.S_ISSOCK(observed.st_mode):
        raise SourceBrokerTransportError("source broker endpoint authority is not a socket")
    mode = stat.S_IMODE(observed.st_mode)
    identity = SocketEndpointIdentity(
        device=observed.st_dev,
        inode=observed.st_ino,
        owner_uid=observed.st_uid,
        group_gid=observed.st_gid,
        mode=mode,
    )
    expected = (
        endpoint.owner_uid,
        endpoint.group_gid,
        endpoint.mode,
    )
    if (identity.owner_uid, identity.group_gid, identity.mode) != expected:
        raise SourceBrokerTransportError("source broker endpoint authority metadata is invalid")
    return identity


def _unlink_stale_endpoint(
    directory: _DirectoryAuthority,
    endpoint: SocketEndpointPolicy,
) -> None:
    try:
        _endpoint_identity_at(directory, endpoint)
    except SourceBrokerTransportError:
        try:
            os.stat(endpoint.path.name, dir_fd=directory.fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        except OSError as stat_exc:
            raise SourceBrokerTransportError(
                "source broker stale endpoint is unavailable"
            ) from stat_exc
        raise
    os.unlink(endpoint.path.name, dir_fd=directory.fd)


def _safe_transport_error(exc: SourceBrokerTransportError) -> str:
    message = str(exc)
    if message == "peer credentials are not allowed":
        return message
    if message.startswith("source broker frame"):
        return "source broker frame is invalid"
    if message.startswith("source broker JSON"):
        return "source broker request is invalid"
    return "source broker request was rejected"
