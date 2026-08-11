"""Closed canonical protocol and endpoint authority for Source Broker transport."""

from __future__ import annotations

import os
import socket
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, NoReturn, TypeVar

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from rquant.adapter_manifest import SourceUsePlan
from rquant.source_broker import (
    QuotaReservation,
    SourceCallReceipt,
    SourceUseStatement,
)
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_json_loads

MAX_SOURCE_BROKER_FRAME_BYTES = 1024 * 1024
_FRAME_HEADER_BYTES = 4
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_SAGA_ID_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,126}[A-Za-z0-9])?$"
_IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9](?:[A-Za-z0-9._:-]{0,126}[A-Za-z0-9])?$"
ModelT = TypeVar("ModelT")


class SourceBrokerTransportError(RuntimeError):
    """The local Source Broker transport is malformed or unsafe."""


class SourceBrokerTransportRemoteError(SourceBrokerTransportError):
    """The authenticated Source Broker service explicitly rejected a request."""


class _WireModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        strict=True,
        str_strip_whitespace=False,
        validate_by_alias=False,
        validate_by_name=True,
    )


class DailyBarsCallRequest(_WireModel):
    """The only provider call shape admitted by transport protocol v1."""

    request_type: Literal["daily_bars_v1"] = "daily_bars_v1"
    trade_date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    market: str | None = Field(default=None, pattern=r"^[A-Za-z0-9._:-]{1,32}$")


class _BoundRequest(_WireModel):
    schema_version: Literal[1] = 1
    operation_id: str = Field(pattern=_HASH_PATTERN)
    saga_id: str = Field(pattern=_SAGA_ID_PATTERN, min_length=1, max_length=128)
    attempt_identity_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)
    plan: SourceUsePlan

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version_type(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("schema_version must be the integer 1")
        return value

    @model_validator(mode="after")
    def validate_plan_binding(self) -> _BoundRequest:
        if self.plan_hash != self.plan.plan_hash:
            raise ValueError("plan_hash does not match the canonical signed plan")
        return self


class SourceBrokerStartRequest(_BoundRequest):
    operation: Literal["start"] = "start"


class SourceBrokerCallRequest(_BoundRequest):
    operation: Literal["call"] = "call"
    idempotency_key: str = Field(
        pattern=_IDEMPOTENCY_KEY_PATTERN,
        min_length=1,
        max_length=128,
    )
    call_request: DailyBarsCallRequest


class SourceBrokerFinalizeRequest(_BoundRequest):
    operation: Literal["finalize"] = "finalize"


SourceBrokerTransportRequest = Annotated[
    SourceBrokerStartRequest | SourceBrokerCallRequest | SourceBrokerFinalizeRequest,
    Field(discriminator="operation"),
]


class _BoundResponse(_WireModel):
    schema_version: Literal[1] = 1
    ok: Literal[True] = True
    operation_id: str = Field(pattern=_HASH_PATTERN)
    saga_id: str = Field(pattern=_SAGA_ID_PATTERN, min_length=1, max_length=128)
    attempt_identity_hash: str = Field(pattern=_HASH_PATTERN)
    plan_hash: str = Field(pattern=_HASH_PATTERN)

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version_type(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("schema_version must be the integer 1")
        return value

    @classmethod
    def _binding(cls, request: _BoundRequest) -> dict[str, object]:
        return {
            "operation_id": request.operation_id,
            "saga_id": request.saga_id,
            "attempt_identity_hash": request.attempt_identity_hash,
            "plan_hash": request.plan_hash,
        }


class SourceBrokerStartResponse(_BoundResponse):
    operation: Literal["start"] = "start"
    reservation: QuotaReservation

    @classmethod
    def from_request(
        cls,
        *,
        request: SourceBrokerStartRequest,
        reservation: QuotaReservation,
    ) -> SourceBrokerStartResponse:
        return cls(**cls._binding(request), reservation=reservation)


class SourceBrokerCallResponse(_BoundResponse):
    operation: Literal["call"] = "call"
    receipt: SourceCallReceipt

    @classmethod
    def from_request(
        cls,
        *,
        request: SourceBrokerCallRequest,
        receipt: SourceCallReceipt,
    ) -> SourceBrokerCallResponse:
        return cls(**cls._binding(request), receipt=receipt)


class SourceBrokerFinalizeResponse(_BoundResponse):
    operation: Literal["finalize"] = "finalize"
    statement: SourceUseStatement

    @classmethod
    def from_request(
        cls,
        *,
        request: SourceBrokerFinalizeRequest,
        statement: SourceUseStatement,
    ) -> SourceBrokerFinalizeResponse:
        return cls(**cls._binding(request), statement=statement)


SourceBrokerTransportResponse = Annotated[
    SourceBrokerStartResponse | SourceBrokerCallResponse | SourceBrokerFinalizeResponse,
    Field(discriminator="operation"),
]


class SourceBrokerTransportFailure(_WireModel):
    schema_version: Literal[1] = 1
    ok: Literal[False] = False
    error: str = Field(min_length=1, max_length=256)

    @field_validator("schema_version", mode="before")
    @classmethod
    def validate_schema_version_type(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("schema_version must be the integer 1")
        return value


SourceBrokerWireResponse = SourceBrokerTransportResponse | SourceBrokerTransportFailure

_REQUEST_ADAPTER = TypeAdapter(SourceBrokerTransportRequest)
_RESPONSE_ADAPTER = TypeAdapter(SourceBrokerTransportResponse)
_FAILURE_ADAPTER = TypeAdapter(SourceBrokerTransportFailure)


@dataclass(frozen=True)
class SocketEndpointPolicy:
    """The exact filesystem identity expected for one Unix-domain endpoint."""

    path: Path
    owner_uid: int
    group_gid: int
    mode: int

    def __post_init__(self) -> None:
        candidate = Path(os.path.abspath(self.path))
        if not candidate.is_absolute() or candidate != self.path:
            raise ValueError("source broker socket path must be normalized and absolute")
        if not self.path.name or self.path.name in {".", ".."}:
            raise ValueError("source broker socket name is invalid")
        if self.owner_uid < 0 or self.group_gid < 0:
            raise ValueError("source broker socket owner is invalid")
        if self.mode not in {0o600, 0o660}:
            raise ValueError("source broker socket mode must be 0600 or 0660")


@dataclass(frozen=True)
class SocketEndpointIdentity:
    device: int
    inode: int
    owner_uid: int
    group_gid: int
    mode: int


@dataclass(frozen=True)
class PeerCredentialsPolicy:
    """Allowed Linux kernel credentials for Source Broker callers."""

    allowed_uids: frozenset[int]
    allowed_gids: frozenset[int]
    allowed_pids: frozenset[int] | None = None

    def __post_init__(self) -> None:
        if not self.allowed_uids or not self.allowed_gids:
            raise ValueError("source broker peer policy requires allowed uid and gid")
        values = (*self.allowed_uids, *self.allowed_gids)
        if self.allowed_pids is not None:
            if not self.allowed_pids:
                raise ValueError("source broker allowed pid set cannot be empty")
            values += tuple(self.allowed_pids)
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError("source broker peer policy values are invalid")

    def allows(self, *, pid: int, uid: int, gid: int) -> bool:
        return (
            pid > 0
            and uid in self.allowed_uids
            and gid in self.allowed_gids
            and (self.allowed_pids is None or pid in self.allowed_pids)
        )


@dataclass(frozen=True)
class ServerCredentialsPolicy:
    """The connected Source Broker process identity required by a client."""

    expected_uid: int
    expected_gid: int
    expected_pid: int | None = None

    def __post_init__(self) -> None:
        values = (self.expected_uid, self.expected_gid)
        if any(type(value) is not int or value < 0 for value in values):
            raise ValueError("source broker server uid or gid is invalid")
        if self.expected_pid is not None and (
            type(self.expected_pid) is not int or self.expected_pid <= 0
        ):
            raise ValueError("source broker server pid is invalid")

    def allows(self, *, pid: int, uid: int, gid: int) -> bool:
        return (
            pid > 0
            and uid == self.expected_uid
            and gid == self.expected_gid
            and (self.expected_pid is None or pid == self.expected_pid)
        )


def source_broker_peer_credentials_supported() -> bool:
    return sys.platform.startswith("linux") and hasattr(socket, "SO_PEERCRED")


def require_linux_source_broker_transport() -> None:
    if not source_broker_peer_credentials_supported():
        raise SourceBrokerTransportError(
            "Linux SO_PEERCRED is required for Source Broker transport"
        )


def validate_socket_parent(policy: SocketEndpointPolicy) -> None:
    direct_parent = policy.path.parent
    for parent in (direct_parent, *direct_parent.parents):
        try:
            observed = os.lstat(parent)
        except OSError as exc:
            raise SourceBrokerTransportError("source broker socket parent is unavailable") from exc
        if not stat.S_ISDIR(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
            raise SourceBrokerTransportError("source broker socket parent is unsafe")
        if observed.st_uid not in {0, policy.owner_uid}:
            raise SourceBrokerTransportError("source broker socket parent owner is unsafe")
        if observed.st_mode & 0o022 and (
            parent == direct_parent or not observed.st_mode & stat.S_ISVTX
        ):
            raise SourceBrokerTransportError("source broker socket parent mode is unsafe")


def validate_socket_endpoint(
    policy: SocketEndpointPolicy,
    *,
    expected_identity: SocketEndpointIdentity | None = None,
) -> SocketEndpointIdentity:
    validate_socket_parent(policy)
    try:
        observed = os.lstat(policy.path)
    except OSError as exc:
        raise SourceBrokerTransportError("source broker socket endpoint is unavailable") from exc
    identity = _socket_endpoint_identity(observed, policy)
    if expected_identity is not None and identity != expected_identity:
        raise SourceBrokerTransportError("source broker socket endpoint was replaced")
    return identity


def verify_connected_server_authority(
    *,
    server_pid: int,
    endpoint: SocketEndpointPolicy,
    endpoint_identity: SocketEndpointIdentity,
) -> None:
    """Require the connected process to own the listener bound to the pinned pathname."""

    require_linux_source_broker_transport()
    current = validate_socket_endpoint(endpoint, expected_identity=endpoint_identity)
    if current != endpoint_identity:
        raise SourceBrokerTransportError("source broker endpoint authority changed")
    listener_inodes = _linux_listener_inodes(endpoint.path)
    if not listener_inodes:
        raise SourceBrokerTransportError("source broker listener authority is unavailable")
    try:
        entries = tuple(os.scandir(Path("/proc") / str(server_pid) / "fd"))
    except OSError as exc:
        raise SourceBrokerTransportError(
            "source broker server fd authority is unavailable"
        ) from exc
    owned_inodes: set[int] = set()
    for entry in entries:
        try:
            target = os.readlink(entry.path)
        except OSError:
            continue
        if target.startswith("socket:[") and target.endswith("]"):
            try:
                owned_inodes.add(int(target[8:-1]))
            except ValueError:
                continue
    if not listener_inodes & owned_inodes:
        raise SourceBrokerTransportError(
            "connected process does not own the Source Broker listener"
        )


def encode_request(request: SourceBrokerTransportRequest) -> bytes:
    return _encode_model(request)


def decode_request(payload: bytes) -> SourceBrokerTransportRequest:
    return _decode_validated(payload, _REQUEST_ADAPTER, label="request")


def encode_response(response: SourceBrokerTransportResponse) -> bytes:
    return _encode_model(response)


def encode_failure(error: str) -> bytes:
    return _encode_model(SourceBrokerTransportFailure(error=error))


def decode_response(payload: bytes) -> SourceBrokerWireResponse:
    value = _decode_json_object(payload)
    adapter: TypeAdapter[SourceBrokerTransportResponse] | TypeAdapter[SourceBrokerTransportFailure]
    adapter = _FAILURE_ADAPTER if value.get("ok") is False else _RESPONSE_ADAPTER
    return _validate_and_require_exact(payload, value, adapter, label="response")


def validate_response_binding(
    *,
    request: SourceBrokerTransportRequest,
    response: SourceBrokerTransportResponse,
) -> None:
    if (
        response.operation != request.operation
        or response.operation_id != request.operation_id
        or response.saga_id != request.saga_id
        or response.attempt_identity_hash != request.attempt_identity_hash
        or response.plan_hash != request.plan_hash
    ):
        raise SourceBrokerTransportError("source broker response binding is invalid")


def write_frame(connection: socket.socket, payload: bytes) -> None:
    if not payload or len(payload) > MAX_SOURCE_BROKER_FRAME_BYTES:
        raise SourceBrokerTransportError("source broker frame size is invalid")
    try:
        connection.sendall(len(payload).to_bytes(_FRAME_HEADER_BYTES, "big") + payload)
    except OSError as exc:
        raise SourceBrokerTransportError("source broker frame write failed") from exc


def read_frame(connection: socket.socket) -> bytes:
    header = _recv_exact(connection, _FRAME_HEADER_BYTES)
    size = int.from_bytes(header, "big", signed=False)
    if not 0 < size <= MAX_SOURCE_BROKER_FRAME_BYTES:
        raise SourceBrokerTransportError("source broker frame size is invalid")
    return _recv_exact(connection, size)


def _recv_exact(connection: socket.socket, size: int) -> bytes:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        try:
            chunk = connection.recv(remaining)
        except OSError as exc:
            raise SourceBrokerTransportError("source broker frame read failed") from exc
        if not chunk:
            raise SourceBrokerTransportError("source broker frame is truncated")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _decode_validated(
    payload: bytes,
    adapter: TypeAdapter[SourceBrokerTransportRequest],
    *,
    label: str,
) -> SourceBrokerTransportRequest:
    value = _decode_json_object(payload)
    return _validate_and_require_exact(payload, value, adapter, label=label)


def _validate_and_require_exact(
    payload: bytes,
    value: dict[str, object],
    adapter: TypeAdapter[ModelT],
    *,
    label: str,
) -> ModelT:
    try:
        validated = adapter.validate_python(value)
    except ValidationError as exc:
        raise SourceBrokerTransportError(f"source broker {label} validation failed") from exc
    if payload != _encode_model(validated):
        raise SourceBrokerTransportError(
            f"source broker {label} is not the exact validated canonical JSON"
        )
    return validated


def _decode_json_object(payload: bytes) -> dict[str, object]:
    if not payload or len(payload) > MAX_SOURCE_BROKER_FRAME_BYTES:
        raise SourceBrokerTransportError("source broker JSON payload size is invalid")
    try:
        value = strict_json_loads(payload, parse_constant=_reject_nonfinite_json)
    except (StrictJsonError, UnicodeDecodeError) as exc:
        raise SourceBrokerTransportError(f"source broker JSON is invalid: {exc}") from exc
    if not isinstance(value, dict):
        raise SourceBrokerTransportError("source broker JSON must be an object")
    return value


def _encode_model(model: BaseModel) -> bytes:
    try:
        return canonical_json_bytes(model.model_dump(mode="json"))
    except (TypeError, ValueError) as exc:
        raise SourceBrokerTransportError("source broker model cannot be encoded") from exc


def _reject_nonfinite_json(value: str) -> NoReturn:
    raise StrictJsonError(f"non-finite JSON value is forbidden: {value}")


def _socket_endpoint_identity(
    observed: os.stat_result,
    policy: SocketEndpointPolicy,
) -> SocketEndpointIdentity:
    if stat.S_ISLNK(observed.st_mode):
        raise SourceBrokerTransportError("source broker socket endpoint is a symlink")
    if not stat.S_ISSOCK(observed.st_mode):
        raise SourceBrokerTransportError("source broker socket endpoint is not a socket")
    mode = stat.S_IMODE(observed.st_mode)
    if observed.st_uid != policy.owner_uid:
        raise SourceBrokerTransportError("source broker socket endpoint owner is invalid")
    if observed.st_gid != policy.group_gid:
        raise SourceBrokerTransportError("source broker socket endpoint group is invalid")
    if mode != policy.mode:
        raise SourceBrokerTransportError("source broker socket endpoint mode is invalid")
    return SocketEndpointIdentity(
        device=observed.st_dev,
        inode=observed.st_ino,
        owner_uid=observed.st_uid,
        group_gid=observed.st_gid,
        mode=mode,
    )


def _linux_listener_inodes(path: Path) -> set[int]:
    try:
        payload = Path("/proc/net/unix").read_text(encoding="ascii")
    except OSError as exc:
        raise SourceBrokerTransportError("Linux Unix listener table is unavailable") from exc
    result: set[int] = set()
    expected = str(path)
    for line in payload.splitlines()[1:]:
        columns = line.split(maxsplit=7)
        if len(columns) != 8:
            continue
        _number, _refs, _protocol, flags, socket_type, _state, inode, bound = columns
        if bound != expected or socket_type != "0001":
            continue
        try:
            listening = int(flags, 16) & 0x00010000
            parsed_inode = int(inode)
        except ValueError:
            continue
        if listening:
            result.add(parsed_inode)
    return result
