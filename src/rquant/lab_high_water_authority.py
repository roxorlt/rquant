"""Independent monotonic compare-and-advance high-water authority for the Lab ledger.

The authority is a separate process that exclusively owns the durable high-water
state.  The Lab runtime holds no write or rollback authority over that state: it
can only submit signed compare-and-advance requests over a Unix socket and must
fail closed whenever the authority is unreachable, degraded, or refuses.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import os
import re
import socket
import stat
import struct
import sys
import threading
from collections import OrderedDict
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, Self, TypeAlias
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_model_validate_json,
)

HIGH_WATER_GENESIS_HASH = "0" * 64

_LOG_NAME = "chain.jsonl"
_CURRENT_NAME = "current.json"
_CHECKPOINT_NAME = "checkpoint.json"
_WRITER_LOCK_NAME = ".writer.lock"
_MAX_LINE_BYTES = 16_384
_MAX_REQUEST_BYTES = 65_536
_MAX_RESPONSE_BYTES = 262_144
_REPLAY_JOURNAL_LIMIT = 4_096
_KEY_ID_PATTERN = r"[A-Za-z0-9._-]{1,128}"
_PORTABLE_UNIX_SOCKET_PATH_BYTES = 100

GraphReceiptKind: TypeAlias = Literal["incremental", "full"]
_ReasonCode: TypeAlias = Literal[
    "ok",
    "rollback",
    "identity",
    "identity_rotation",
    "conflict",
    "replay",
    "untrusted",
    "invalid",
]


class LabHighWaterAuthorityError(RuntimeError):
    """The high-water authority refused, degraded, or could not be reached."""


class LabHighWaterRollbackError(LabHighWaterAuthorityError):
    """The observed Lab ledger is behind the authority's monotonic high water."""


class LabHighWaterIdentityError(LabHighWaterAuthorityError):
    """The observed Lab database identity conflicts with the anchored identity."""


def _bounded_socket_path(raw_path: Path) -> Path:
    """Derive a portable, deterministic socket name without weakening authority state.

    macOS limits Unix domain socket paths to 104 bytes.  The authority state
    remains under its configured root; only the transient transport endpoint is
    relocated to a per-user, mode-0700 directory when necessary.
    """

    path = Path(raw_path)
    encoded = os.fsencode(path)
    if len(encoded) <= _PORTABLE_UNIX_SOCKET_PATH_BYTES:
        return path
    digest = hashlib.sha256(encoded).hexdigest()
    return Path("/tmp") / f"rquant-high-water-{os.geteuid()}" / f"{digest[:32]}.sock"


def _ensure_socket_parent(path: Path) -> None:
    """Create and validate only the per-user transport directory if required."""

    parent = path.parent
    if parent.parent != Path("/tmp") or not parent.name.startswith("rquant-high-water-"):
        return
    try:
        parent.mkdir(mode=0o700, exist_ok=True)
        observed = parent.lstat()
    except OSError as exc:
        raise LabHighWaterAuthorityError(
            "high-water socket parent cannot be prepared safely"
        ) from exc
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise LabHighWaterAuthorityError("high-water socket parent is unsafe")


@dataclass(frozen=True)
class LabHighWaterKey:
    """HMAC credential for one side of the high-water authority protocol."""

    key_id: str
    secret: bytes = field(repr=False)

    def __post_init__(self) -> None:
        if re.fullmatch(_KEY_ID_PATTERN, self.key_id) is None:
            raise ValueError("high-water key_id is invalid")
        if not isinstance(self.secret, bytes) or not 32 <= len(self.secret) <= 4_096:
            raise ValueError("high-water secret must be between 32 and 4096 bytes")


LabHighWaterKeyProvider = Callable[[], LabHighWaterKey]
LabHighWaterTrustedKeyProvider = Callable[[str], "LabHighWaterKey | None"]


class _HighWaterModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


def _validate_database_generation(value: tuple[int, int]) -> None:
    if any(type(item) is not int or item < 0 for item in value):
        raise ValueError("database generation must contain non-negative integers")


class LabHighWaterRecord(_HighWaterModel):
    """One authority-signed monotonic high-water record."""

    schema_version: Literal[1] = 1
    sequence: int = Field(ge=0)
    previous_record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    database_stable_identity: str = Field(min_length=1, max_length=512)
    database_generation: tuple[int, int]
    schema_generation: int = Field(ge=1)
    mutation_epoch: int = Field(ge=0)
    chain_generation: int = Field(ge=0)
    chain_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    graph_receipt_kind: GraphReceiptKind
    graph_receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_identity: str = Field(pattern=r"^[0-9a-f]{40,128}$")
    profile_identity: str = Field(pattern=r"^[0-9a-f]{64,128}$")
    request_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    client_key_id: str = Field(pattern=rf"^{_KEY_ID_PATTERN}$")
    authority_key_id: str = Field(pattern=rf"^{_KEY_ID_PATTERN}$")
    record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_generation(self) -> Self:
        _validate_database_generation(self.database_generation)
        return self

    @classmethod
    def signed(cls, *, key: LabHighWaterKey, **values: object) -> LabHighWaterRecord:
        payload = {
            "schema_version": 1,
            **values,
            "authority_key_id": key.key_id,
        }
        record_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        signature = hmac.new(key.secret, record_hash.encode("ascii"), hashlib.sha256).hexdigest()
        return cls.model_validate({**payload, "record_hash": record_hash, "signature": signature})

    def verify(self, key_provider: LabHighWaterTrustedKeyProvider) -> None:
        payload = self.model_dump(mode="json", exclude={"record_hash", "signature"})
        expected_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        if not hmac.compare_digest(self.record_hash, expected_hash):
            raise LabHighWaterAuthorityError("high-water record hash is invalid")
        key = key_provider(self.authority_key_id)
        if key is None or key.key_id != self.authority_key_id:
            raise LabHighWaterAuthorityError("high-water record authority key is not trusted")
        expected_signature = hmac.new(
            key.secret,
            self.record_hash.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(self.signature, expected_signature):
            raise LabHighWaterAuthorityError("high-water record signature is invalid")


class _SignedEnvelope(_HighWaterModel):
    """Shared canonical-hash + HMAC discipline for wire messages."""

    @classmethod
    def _signed_values(
        cls,
        values: dict[str, object],
        *,
        key: LabHighWaterKey,
        hash_field: str,
        key_field: str,
    ) -> dict[str, object]:
        payload = {**values, key_field: key.key_id}
        content_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        signature = hmac.new(key.secret, content_hash.encode("ascii"), hashlib.sha256).hexdigest()
        return {**payload, hash_field: content_hash, "signature": signature}

    def _verify_envelope(
        self,
        *,
        key_provider: LabHighWaterTrustedKeyProvider,
        hash_field: str,
        key_field: str,
    ) -> None:
        payload = self.model_dump(mode="json", exclude={hash_field, "signature"})
        expected_hash = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        observed_hash = getattr(self, hash_field)
        if not hmac.compare_digest(observed_hash, expected_hash):
            raise LabHighWaterAuthorityError("high-water message hash is invalid")
        key_id = getattr(self, key_field)
        key = key_provider(key_id)
        if key is None or key.key_id != key_id:
            raise LabHighWaterAuthorityError("high-water message key is not trusted")
        expected_signature = hmac.new(
            key.secret,
            observed_hash.encode("ascii"),
            hashlib.sha256,
        ).hexdigest()
        if not hmac.compare_digest(self.signature, expected_signature):
            raise LabHighWaterAuthorityError("high-water message signature is invalid")


class LabHighWaterStatusRequest(_SignedEnvelope):
    schema_version: Literal[1] = 1
    kind: Literal["status"]
    request_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    database_stable_identity: str = Field(min_length=1, max_length=512)
    key_id: str = Field(pattern=rf"^{_KEY_ID_PATTERN}$")
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(pattern=r"^[0-9a-f]{64}$")

    def verify(self, key_provider: LabHighWaterTrustedKeyProvider) -> None:
        self._verify_envelope(
            key_provider=key_provider,
            hash_field="request_hash",
            key_field="key_id",
        )


class LabHighWaterAdvanceRequest(_SignedEnvelope):
    schema_version: Literal[1] = 1
    kind: Literal["advance"]
    request_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    database_stable_identity: str = Field(min_length=1, max_length=512)
    database_generation: tuple[int, int]
    schema_generation: int = Field(ge=1)
    mutation_epoch: int = Field(ge=0)
    chain_generation: int = Field(ge=0)
    chain_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    graph_receipt_kind: GraphReceiptKind
    graph_receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_identity: str = Field(pattern=r"^[0-9a-f]{40,128}$")
    profile_identity: str = Field(pattern=r"^[0-9a-f]{64,128}$")
    expected_sequence: int = Field(ge=-1)
    expected_record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(pattern=rf"^{_KEY_ID_PATTERN}$")
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_generation(self) -> Self:
        _validate_database_generation(self.database_generation)
        return self

    def verify(self, key_provider: LabHighWaterTrustedKeyProvider) -> None:
        self._verify_envelope(
            key_provider=key_provider,
            hash_field="request_hash",
            key_field="key_id",
        )


class LabHighWaterResponse(_SignedEnvelope):
    schema_version: Literal[1] = 1
    request_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    outcome: Literal["advanced", "unchanged", "refused"]
    reason_code: _ReasonCode
    reason: str | None = Field(default=None, max_length=512)
    state: LabHighWaterRecord | None
    authority_key_id: str = Field(pattern=rf"^{_KEY_ID_PATTERN}$")
    response_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def signed(
        cls,
        *,
        key: LabHighWaterKey,
        request_id: str,
        outcome: Literal["advanced", "unchanged", "refused"],
        reason_code: _ReasonCode,
        reason: str | None,
        state: LabHighWaterRecord | None,
    ) -> LabHighWaterResponse:
        values = cls._signed_values(
            {
                "schema_version": 1,
                "request_id": request_id,
                "outcome": outcome,
                "reason_code": reason_code,
                "reason": reason,
                "state": None if state is None else state.model_dump(mode="json"),
            },
            key=key,
            hash_field="response_hash",
            key_field="authority_key_id",
        )
        return cls.model_validate(values)

    def verify(self, key_provider: LabHighWaterTrustedKeyProvider) -> None:
        self._verify_envelope(
            key_provider=key_provider,
            hash_field="response_hash",
            key_field="authority_key_id",
        )


@dataclass(frozen=True)
class LabHighWaterAuthorityServerConfig:
    root: Path
    socket_path: Path
    database_stable_identity: str
    signing_key_provider: LabHighWaterKeyProvider
    trusted_client_key_provider: LabHighWaterTrustedKeyProvider
    allow_identity_rotation: bool = False
    allowed_client_uids: tuple[int, ...] = ()
    max_bytes: int = 4 * 1_024 * 1_024
    max_records: int = 16_384
    request_timeout_seconds: float = 5.0
    accept_poll_seconds: float = 0.2

    def __post_init__(self) -> None:
        object.__setattr__(self, "socket_path", _bounded_socket_path(Path(self.socket_path)))
        if not self.database_stable_identity or len(self.database_stable_identity) > 512:
            raise ValueError("high-water database_stable_identity is invalid")
        if not 1_024 <= self.max_bytes <= 64 * 1_024 * 1_024:
            raise ValueError("high-water max_bytes is outside the safe range")
        if not 2 <= self.max_records <= 65_536:
            raise ValueError("high-water max_records is outside the safe range")
        if not 0.05 <= self.request_timeout_seconds <= 300:
            raise ValueError("high-water request timeout is outside the safe range")
        if not 0.01 <= self.accept_poll_seconds <= 10:
            raise ValueError("high-water accept poll interval is outside the safe range")
        if any(type(uid) is not int or uid < 0 for uid in self.allowed_client_uids):
            raise ValueError("high-water allowed client uids are invalid")


@dataclass(frozen=True)
class LabHighWaterAuthorityClientConfig:
    socket_path: Path
    database_stable_identity: str
    code_identity: str
    profile_identity: str
    signing_key_provider: LabHighWaterKeyProvider
    trusted_authority_key_provider: LabHighWaterTrustedKeyProvider
    timeout_seconds: float = 5.0
    expected_server_uid: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "socket_path", _bounded_socket_path(Path(self.socket_path)))
        if not self.database_stable_identity or len(self.database_stable_identity) > 512:
            raise ValueError("high-water database_stable_identity is invalid")
        if re.fullmatch(r"[0-9a-f]{40,128}", self.code_identity) is None:
            raise ValueError("high-water code_identity must be a SHA")
        if re.fullmatch(r"[0-9a-f]{64,128}", self.profile_identity) is None:
            raise ValueError("high-water profile_identity must be a SHA")
        if not 0.05 <= self.timeout_seconds <= 300:
            raise ValueError("high-water client timeout is outside the safe range")
        if self.expected_server_uid is not None and self.expected_server_uid < 0:
            raise ValueError("high-water expected server uid is invalid")


def _peer_euid(connection: socket.socket) -> int:
    """Read the connected Unix-socket peer's effective UID from the kernel."""

    if sys.platform == "darwin":
        sol_local = getattr(socket, "SOL_LOCAL", 0)
        local_peercred = getattr(socket, "LOCAL_PEERCRED", 0x0001)
        raw = connection.getsockopt(sol_local, local_peercred, 128)
        if len(raw) < 8:
            raise LabHighWaterAuthorityError("peer credential structure is truncated")
        version, uid = struct.unpack_from("=II", raw)
        if version != 0:
            raise LabHighWaterAuthorityError("peer credential structure version is unsupported")
        return uid
    so_peercred = getattr(socket, "SO_PEERCRED", 17)
    raw = connection.getsockopt(socket.SOL_SOCKET, so_peercred, struct.calcsize("=3i"))
    _pid, uid, _gid = struct.unpack("=3i", raw)
    return uid


def _open_verified_root(root: Path) -> int:
    """Open the state root via O_NOFOLLOW and validate identity from the fd only."""

    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        descriptor = os.open(root, flags)
    except OSError as exc:
        raise LabHighWaterAuthorityError(
            "high-water state root cannot be opened safely"
        ) from exc
    try:
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o700
        ):
            raise LabHighWaterAuthorityError(
                "high-water state root must be an owned physical directory with mode 0700"
            )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


class _StoreState:
    __slots__ = ("checkpoint", "records", "needs_truncate")

    def __init__(
        self,
        *,
        checkpoint: LabHighWaterRecord | None,
        records: tuple[LabHighWaterRecord, ...],
        needs_truncate: bool,
    ) -> None:
        self.checkpoint = checkpoint
        self.records = records
        self.needs_truncate = needs_truncate

    @property
    def head(self) -> LabHighWaterRecord | None:
        if self.records:
            return self.records[-1]
        return self.checkpoint


class _LabHighWaterStore:
    """Append-only signed chain with dirfd + O_NOFOLLOW + fstat identity binding."""

    def __init__(
        self,
        root: Path,
        *,
        max_bytes: int,
        max_records: int,
        authority_key_provider: LabHighWaterTrustedKeyProvider,
    ) -> None:
        self.root = Path(root)
        self.max_bytes = max_bytes
        self.max_records = max_records
        self.authority_key_provider = authority_key_provider

    def initialize(self) -> None:
        try:
            os.mkdir(self.root, mode=0o700)
        except FileExistsError:
            pass
        except OSError as exc:
            raise LabHighWaterAuthorityError("high-water state root cannot be created") from exc
        descriptor = _open_verified_root(self.root)
        os.close(descriptor)

    def _read_name(self, descriptor: int, name: str) -> bytes | None:
        try:
            file_descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=descriptor,
            )
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise LabHighWaterAuthorityError(
                f"high-water {name} cannot be opened safely"
            ) from exc
        try:
            before = os.fstat(file_descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_size > self.max_bytes
            ):
                raise LabHighWaterAuthorityError(f"high-water {name} identity is invalid")
            chunks: list[bytes] = []
            remaining = self.max_bytes + 1
            while remaining > 0:
                block = os.read(file_descriptor, min(65_536, remaining))
                if not block:
                    break
                chunks.append(block)
                remaining -= len(block)
            payload = b"".join(chunks)
            after = os.fstat(file_descriptor)
            if len(payload) > self.max_bytes or (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                raise LabHighWaterAuthorityError(f"high-water {name} changed while reading")
            return payload
        finally:
            os.close(file_descriptor)

    def _write_atomic(self, descriptor: int, name: str, payload: bytes) -> None:
        temporary = f".{name}.{uuid4().hex}.tmp"
        try:
            file_descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                0o600,
                dir_fd=descriptor,
            )
            try:
                os.write(file_descriptor, payload)
                os.fsync(file_descriptor)
            finally:
                os.close(file_descriptor)
            os.replace(temporary, name, src_dir_fd=descriptor, dst_dir_fd=descriptor)
            os.fsync(descriptor)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=descriptor)

    def _append_log(self, descriptor: int, payload: bytes) -> None:
        file_descriptor = os.open(
            _LOG_NAME,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_APPEND
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=descriptor,
        )
        try:
            observed = os.fstat(file_descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
                or observed.st_uid != os.geteuid()
                or stat.S_IMODE(observed.st_mode) != 0o600
                or observed.st_size + len(payload) > self.max_bytes
            ):
                raise LabHighWaterAuthorityError("high-water chain log exceeds its bounds")
            os.write(file_descriptor, payload)
            os.fsync(file_descriptor)
        finally:
            os.close(file_descriptor)
        os.fsync(descriptor)

    def _parse_record(self, line: bytes, *, label: str) -> LabHighWaterRecord:
        if len(line) > _MAX_LINE_BYTES:
            raise LabHighWaterAuthorityError(f"high-water {label} exceeds its size bound")
        try:
            record = strict_model_validate_json(LabHighWaterRecord, line)
        except Exception as exc:
            raise LabHighWaterAuthorityError(f"high-water {label} is invalid") from exc
        if canonical_model_json_bytes(record) != line:
            raise LabHighWaterAuthorityError(f"high-water {label} is not canonical")
        record.verify(self.authority_key_provider)
        return record

    def load(self, descriptor: int) -> _StoreState:
        checkpoint_payload = self._read_name(descriptor, _CHECKPOINT_NAME)
        checkpoint = (
            self._parse_record(checkpoint_payload, label="checkpoint")
            if checkpoint_payload is not None
            else None
        )
        chain_payload = self._read_name(descriptor, _LOG_NAME)
        raw_records: list[LabHighWaterRecord] = []
        if chain_payload:
            if not chain_payload.endswith(b"\n"):
                raise LabHighWaterAuthorityError("high-water chain log is incomplete")
            lines = chain_payload[:-1].split(b"\n")
            if len(lines) > self.max_records:
                raise LabHighWaterAuthorityError("high-water chain log record count is invalid")
            for line in lines:
                raw_records.append(self._parse_record(line, label="chain record"))
        needs_truncate = False
        if checkpoint is None:
            expected_sequence = 0
            previous_hash = HIGH_WATER_GENESIS_HASH
        else:
            expected_sequence = checkpoint.sequence + 1
            previous_hash = checkpoint.record_hash
        if (
            checkpoint is not None
            and raw_records
            and raw_records[0].sequence != expected_sequence
        ):
            self._verify_stale_chain(raw_records, checkpoint=checkpoint)
            return _StoreState(checkpoint=checkpoint, records=(), needs_truncate=True)
        for record in raw_records:
            if record.sequence != expected_sequence or (
                record.previous_record_hash != previous_hash
            ):
                raise LabHighWaterAuthorityError("high-water chain linkage is invalid")
            expected_sequence += 1
            previous_hash = record.record_hash
        state = _StoreState(
            checkpoint=checkpoint,
            records=tuple(raw_records),
            needs_truncate=needs_truncate,
        )
        self._verify_current(descriptor, state)
        return state

    def _verify_stale_chain(
        self,
        raw_records: list[LabHighWaterRecord],
        *,
        checkpoint: LabHighWaterRecord,
    ) -> None:
        """Accept a chain superseded by a checkpoint written just before a crash."""

        if raw_records[-1] != checkpoint:
            raise LabHighWaterAuthorityError("high-water chain conflicts with its checkpoint")
        expected_sequence = raw_records[0].sequence
        previous_hash = raw_records[0].previous_record_hash
        for record in raw_records:
            if record.sequence != expected_sequence or (
                record.previous_record_hash != previous_hash
            ):
                raise LabHighWaterAuthorityError("high-water stale chain linkage is invalid")
            expected_sequence += 1
            previous_hash = record.record_hash

    def _verify_current(self, descriptor: int, state: _StoreState) -> None:
        payload = self._read_name(descriptor, _CURRENT_NAME)
        head = state.head
        if payload is None:
            return
        if head is None:
            raise LabHighWaterAuthorityError("high-water current exists without a chain")
        pointer = self._parse_record(payload, label="current pointer")
        if pointer.sequence > head.sequence:
            raise LabHighWaterAuthorityError("high-water current exceeds its chain")
        if pointer.sequence == head.sequence and pointer != head:
            raise LabHighWaterAuthorityError("high-water current conflicts with its chain")

    def read_state(self) -> _StoreState:
        descriptor = _open_verified_root(self.root)
        try:
            return self.load(descriptor)
        finally:
            os.close(descriptor)

    def append(self, record: LabHighWaterRecord) -> None:
        """Append one signed record under the writer lock, compacting when full."""

        descriptor = _open_verified_root(self.root)
        lock_descriptor = -1
        try:
            lock_descriptor = os.open(
                _WRITER_LOCK_NAME,
                os.O_RDWR
                | os.O_CREAT
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                0o600,
                dir_fd=descriptor,
            )
            lock_identity = os.fstat(lock_descriptor)
            if not stat.S_ISREG(lock_identity.st_mode) or lock_identity.st_nlink != 1:
                raise LabHighWaterAuthorityError("high-water writer lock is invalid")
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
            state = self.load(descriptor)
            head = state.head
            expected_sequence = 0 if head is None else head.sequence + 1
            expected_previous = HIGH_WATER_GENESIS_HASH if head is None else head.record_hash
            if record.sequence != expected_sequence or (
                record.previous_record_hash != expected_previous
            ):
                raise LabHighWaterAuthorityError("high-water append is not contiguous")
            if state.needs_truncate:
                self._write_atomic(descriptor, _LOG_NAME, b"")
                state = _StoreState(
                    checkpoint=state.checkpoint,
                    records=(),
                    needs_truncate=False,
                )
            if len(state.records) + 1 >= self.max_records and head is not None:
                self._write_atomic(
                    descriptor,
                    _CHECKPOINT_NAME,
                    canonical_model_json_bytes(head),
                )
                self._write_atomic(descriptor, _LOG_NAME, b"")
            self._append_log(descriptor, canonical_model_json_bytes(record) + b"\n")
            self._write_atomic(descriptor, _CURRENT_NAME, canonical_model_json_bytes(record))
        except OSError as exc:
            raise LabHighWaterAuthorityError("high-water state could not be persisted") from exc
        finally:
            if lock_descriptor >= 0:
                with suppress(OSError):
                    fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)
            os.close(descriptor)


class LabHighWaterAuthorityServer:
    """Serve signed monotonic compare-and-advance requests over a Unix socket."""

    def __init__(self, config: LabHighWaterAuthorityServerConfig) -> None:
        self.config = config
        self.store = _LabHighWaterStore(
            config.root,
            max_bytes=config.max_bytes,
            max_records=config.max_records,
            authority_key_provider=self._authority_verification_key,
        )
        self._listener: socket.socket | None = None
        self._replay_journal: OrderedDict[str, tuple[str, bytes]] = OrderedDict()
        self._journal_lock = threading.Lock()

    def _authority_verification_key(self, key_id: str) -> LabHighWaterKey | None:
        key = self.config.signing_key_provider()
        return key if key.key_id == key_id else None

    def bind(self) -> None:
        self.store.initialize()
        self.store.read_state()
        socket_path = Path(self.config.socket_path)
        _ensure_socket_parent(socket_path)
        with suppress(FileNotFoundError):
            observed = os.lstat(socket_path)
            if not stat.S_ISSOCK(observed.st_mode):
                raise LabHighWaterAuthorityError(
                    "high-water socket path is occupied by a non-socket file"
                )
            os.unlink(socket_path)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(socket_path))
            os.chmod(socket_path, 0o600)
            listener.listen(16)
            listener.settimeout(self.config.accept_poll_seconds)
        except OSError as exc:
            listener.close()
            raise LabHighWaterAuthorityError("high-water socket cannot be bound") from exc
        self._listener = listener

    def close(self) -> None:
        listener = self._listener
        self._listener = None
        if listener is not None:
            with suppress(OSError):
                listener.close()
        with suppress(FileNotFoundError, OSError):
            observed = os.lstat(self.config.socket_path)
            if stat.S_ISSOCK(observed.st_mode):
                os.unlink(self.config.socket_path)

    def serve_forever(self, *, stop: threading.Event | None = None) -> None:
        listener = self._listener
        if listener is None:
            raise LabHighWaterAuthorityError("high-water server is not bound")
        while stop is None or not stop.is_set():
            try:
                connection, _address = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            try:
                self._handle_connection(connection)
            except Exception:
                pass
            finally:
                with suppress(OSError):
                    connection.close()

    def _handle_connection(self, connection: socket.socket) -> None:
        connection.settimeout(self.config.request_timeout_seconds)
        allowed = self.config.allowed_client_uids or (os.geteuid(),)
        if _peer_euid(connection) not in allowed:
            return
        payload = _receive_bounded(connection, _MAX_REQUEST_BYTES)
        if payload is None:
            return
        response_line = self._respond(payload)
        if response_line is not None:
            connection.sendall(response_line)
        with suppress(OSError):
            connection.shutdown(socket.SHUT_WR)

    def _respond(self, payload: bytes) -> bytes | None:
        try:
            document = strict_model_validate_json(_RequestKindProbe, _probe_bytes(payload))
        except Exception:
            return None
        request_id = document.request_id
        try:
            if document.kind == "status":
                request = strict_model_validate_json(LabHighWaterStatusRequest, payload)
                request.verify(self.config.trusted_client_key_provider)
                return self._handle_status(request)
            request = strict_model_validate_json(LabHighWaterAdvanceRequest, payload)
            request.verify(self.config.trusted_client_key_provider)
            return self._handle_advance(request)
        except LabHighWaterAuthorityError as exc:
            return self._refused(request_id, code="untrusted", reason=str(exc))
        except Exception:
            return self._refused(request_id, code="invalid", reason="high-water request invalid")

    def _refused(self, request_id: str, *, code: _ReasonCode, reason: str) -> bytes:
        response = LabHighWaterResponse.signed(
            key=self.config.signing_key_provider(),
            request_id=request_id,
            outcome="refused",
            reason_code=code,
            reason=reason[:512],
            state=None,
        )
        return canonical_model_json_bytes(response) + b"\n"

    def _completed(
        self,
        request_id: str,
        *,
        outcome: Literal["advanced", "unchanged"],
        state: LabHighWaterRecord,
    ) -> bytes:
        response = LabHighWaterResponse.signed(
            key=self.config.signing_key_provider(),
            request_id=request_id,
            outcome=outcome,
            reason_code="ok",
            reason=None,
            state=state,
        )
        return canonical_model_json_bytes(response) + b"\n"

    def _handle_status(self, request: LabHighWaterStatusRequest) -> bytes:
        if request.database_stable_identity != self.config.database_stable_identity:
            return self._refused(
                request.request_id,
                code="identity",
                reason="high-water stable identity conflicts",
            )
        state = self.store.read_state()
        head = state.head
        response = LabHighWaterResponse.signed(
            key=self.config.signing_key_provider(),
            request_id=request.request_id,
            outcome="unchanged",
            reason_code="ok",
            reason=None,
            state=head,
        )
        return canonical_model_json_bytes(response) + b"\n"

    def _handle_advance(self, request: LabHighWaterAdvanceRequest) -> bytes:
        if request.database_stable_identity != self.config.database_stable_identity:
            return self._refused(
                request.request_id,
                code="identity",
                reason="high-water stable identity conflicts",
            )
        with self._journal_lock:
            journaled = self._replay_journal.get(request.request_id)
            if journaled is not None:
                journaled_hash, journaled_response = journaled
                if journaled_hash == request.request_hash:
                    return journaled_response
                return self._refused(
                    request.request_id,
                    code="replay",
                    reason="high-water request id was replayed with different content",
                )
        state = self.store.read_state()
        head = state.head
        if head is not None and head.request_id == request.request_id:
            if self._request_matches_head(request, head):
                return self._journaled(request, self._completed(
                    request.request_id, outcome="unchanged", state=head
                ))
            return self._refused(
                request.request_id,
                code="replay",
                reason="high-water request id was replayed with different content",
            )
        refusal = self._check_monotonic(request, head)
        if refusal is not None:
            return refusal
        expected_sequence = -1 if head is None else head.sequence
        expected_hash = HIGH_WATER_GENESIS_HASH if head is None else head.record_hash
        if (
            request.expected_sequence != expected_sequence
            or request.expected_record_hash != expected_hash
        ):
            return self._refused(
                request.request_id,
                code="conflict",
                reason="high-water compare-and-advance expectation is stale",
            )
        record = LabHighWaterRecord.signed(
            key=self.config.signing_key_provider(),
            sequence=0 if head is None else head.sequence + 1,
            previous_record_hash=HIGH_WATER_GENESIS_HASH if head is None else head.record_hash,
            database_stable_identity=request.database_stable_identity,
            database_generation=request.database_generation,
            schema_generation=request.schema_generation,
            mutation_epoch=request.mutation_epoch,
            chain_generation=request.chain_generation,
            chain_head_hash=request.chain_head_hash,
            graph_receipt_kind=request.graph_receipt_kind,
            graph_receipt_hash=request.graph_receipt_hash,
            code_identity=request.code_identity,
            profile_identity=request.profile_identity,
            request_id=request.request_id,
            client_key_id=request.key_id,
        )
        self.store.append(record)
        return self._journaled(request, self._completed(
            request.request_id, outcome="advanced", state=record
        ))

    def _journaled(self, request: LabHighWaterAdvanceRequest, response_line: bytes) -> bytes:
        with self._journal_lock:
            self._replay_journal[request.request_id] = (request.request_hash, response_line)
            while len(self._replay_journal) > _REPLAY_JOURNAL_LIMIT:
                self._replay_journal.popitem(last=False)
        return response_line

    @staticmethod
    def _request_matches_head(
        request: LabHighWaterAdvanceRequest,
        head: LabHighWaterRecord,
    ) -> bool:
        return (
            request.database_generation,
            request.schema_generation,
            request.mutation_epoch,
            request.chain_generation,
            request.chain_head_hash,
            request.graph_receipt_kind,
            request.graph_receipt_hash,
            request.code_identity,
            request.profile_identity,
            request.key_id,
        ) == (
            head.database_generation,
            head.schema_generation,
            head.mutation_epoch,
            head.chain_generation,
            head.chain_head_hash,
            head.graph_receipt_kind,
            head.graph_receipt_hash,
            head.code_identity,
            head.profile_identity,
            head.client_key_id,
        )

    def _check_monotonic(
        self,
        request: LabHighWaterAdvanceRequest,
        head: LabHighWaterRecord | None,
    ) -> bytes | None:
        if head is None:
            return None
        if head.database_stable_identity != request.database_stable_identity:
            return self._refused(
                request.request_id,
                code="identity",
                reason="high-water stable identity conflicts",
            )
        if head.database_generation != request.database_generation:
            return self._refused(
                request.request_id,
                code="identity",
                reason="high-water database generation changed",
            )
        if head.schema_generation != request.schema_generation:
            return self._refused(
                request.request_id,
                code="conflict",
                reason="high-water schema generation conflicts",
            )
        identity_changed = (
            head.code_identity != request.code_identity
            or head.profile_identity != request.profile_identity
        )
        if identity_changed and not self.config.allow_identity_rotation:
            return self._refused(
                request.request_id,
                code="identity_rotation",
                reason="high-water code or profile identity conflicts",
            )
        if (
            request.chain_generation < head.chain_generation
            or request.mutation_epoch < head.mutation_epoch
        ):
            return self._refused(
                request.request_id,
                code="rollback",
                reason="high-water observed ledger rolled back",
            )
        if request.chain_generation == head.chain_generation and (
            request.mutation_epoch != head.mutation_epoch
            or request.chain_head_hash != head.chain_head_hash
        ):
            return self._refused(
                request.request_id,
                code="conflict",
                reason="high-water chain changed in place",
            )
        return None


class _RequestKindProbe(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)

    kind: Literal["status", "advance"]
    request_id: str = Field(pattern=r"^[0-9a-f]{32}$")


def _probe_bytes(payload: bytes) -> bytes:
    return payload


def _receive_bounded(connection: socket.socket, limit: int) -> bytes | None:
    chunks: list[bytes] = []
    received = 0
    while received <= limit:
        try:
            block = connection.recv(65_536)
        except OSError:
            return None
        if not block:
            break
        chunks.append(block)
        received += len(block)
        if block.endswith(b"\n"):
            break
    if received > limit:
        return None
    payload = b"".join(chunks)
    if not payload.endswith(b"\n"):
        return None
    return payload[:-1]


class LabHighWaterAuthorityClient:
    """Fail-closed client capability for the external high-water authority."""

    def __init__(self, config: LabHighWaterAuthorityClientConfig) -> None:
        self.config = config

    def status(self) -> LabHighWaterRecord | None:
        request_id = uuid4().hex
        payload = LabHighWaterStatusRequest.model_validate(
            _SignedEnvelope._signed_values(
                {
                    "schema_version": 1,
                    "kind": "status",
                    "request_id": request_id,
                    "database_stable_identity": self.config.database_stable_identity,
                },
                key=self.config.signing_key_provider(),
                hash_field="request_hash",
                key_field="key_id",
            )
        )
        response = self._exchange(payload, request_id=request_id)
        self._raise_if_refused(response)
        state = response.state
        if state is not None:
            state.verify(self.config.trusted_authority_key_provider)
            if state.database_stable_identity != self.config.database_stable_identity:
                raise LabHighWaterAuthorityError(
                    "high-water authority returned a foreign stable identity"
                )
        return state

    def observe(
        self,
        *,
        database_generation: tuple[int, int],
        schema_generation: int,
        mutation_epoch: int,
        chain_generation: int,
        chain_head_hash: str,
        graph_receipt_kind: GraphReceiptKind,
        graph_receipt_hash: str,
    ) -> LabHighWaterRecord:
        head = self.status()
        if head is not None:
            if head.database_generation != database_generation:
                raise LabHighWaterIdentityError(
                    "high-water anchored database generation conflicts with the observed ledger"
                )
            if head.schema_generation != schema_generation:
                raise LabHighWaterAuthorityError(
                    "high-water anchored schema generation conflicts with the observed ledger"
                )
            if (
                chain_generation < head.chain_generation
                or mutation_epoch < head.mutation_epoch
            ):
                raise LabHighWaterRollbackError(
                    "observed Lab ledger is behind the high-water authority"
                )
            if chain_generation == head.chain_generation:
                if (
                    mutation_epoch != head.mutation_epoch
                    or chain_head_hash != head.chain_head_hash
                ):
                    raise LabHighWaterAuthorityError(
                        "observed Lab chain changed in place behind the high-water authority"
                    )
                if (
                    graph_receipt_kind == head.graph_receipt_kind
                    and graph_receipt_hash == head.graph_receipt_hash
                    and head.code_identity == self.config.code_identity
                    and head.profile_identity == self.config.profile_identity
                ):
                    return head
        expected_sequence = -1 if head is None else head.sequence
        expected_record_hash = HIGH_WATER_GENESIS_HASH if head is None else head.record_hash
        request_id = uuid4().hex
        payload = LabHighWaterAdvanceRequest.model_validate(
            _SignedEnvelope._signed_values(
                {
                    "schema_version": 1,
                    "kind": "advance",
                    "request_id": request_id,
                    "database_stable_identity": self.config.database_stable_identity,
                    "database_generation": list(database_generation),
                    "schema_generation": schema_generation,
                    "mutation_epoch": mutation_epoch,
                    "chain_generation": chain_generation,
                    "chain_head_hash": chain_head_hash,
                    "graph_receipt_kind": graph_receipt_kind,
                    "graph_receipt_hash": graph_receipt_hash,
                    "code_identity": self.config.code_identity,
                    "profile_identity": self.config.profile_identity,
                    "expected_sequence": expected_sequence,
                    "expected_record_hash": expected_record_hash,
                },
                key=self.config.signing_key_provider(),
                hash_field="request_hash",
                key_field="key_id",
            )
        )
        response = self._exchange(payload, request_id=request_id)
        self._raise_if_refused(response)
        state = response.state
        if state is None:
            raise LabHighWaterAuthorityError("high-water advance returned no state")
        state.verify(self.config.trusted_authority_key_provider)
        if (
            state.database_stable_identity != self.config.database_stable_identity
            or state.database_generation != database_generation
            or state.schema_generation != schema_generation
            or state.mutation_epoch != mutation_epoch
            or state.chain_generation != chain_generation
            or state.chain_head_hash != chain_head_hash
        ):
            raise LabHighWaterAuthorityError(
                "high-water advance state conflicts with the observed ledger"
            )
        return state

    def _raise_if_refused(self, response: LabHighWaterResponse) -> None:
        if response.outcome != "refused":
            return
        reason = response.reason or "high-water authority refused"
        if response.reason_code == "rollback":
            raise LabHighWaterRollbackError(reason)
        if response.reason_code == "identity":
            raise LabHighWaterIdentityError(reason)
        raise LabHighWaterAuthorityError(reason)

    def _exchange(
        self,
        request: LabHighWaterStatusRequest | LabHighWaterAdvanceRequest,
        *,
        request_id: str,
    ) -> LabHighWaterResponse:
        line = canonical_model_json_bytes(request) + b"\n"
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(self.config.timeout_seconds)
                connection.connect(str(self.config.socket_path))
                expected_uid = (
                    os.geteuid()
                    if self.config.expected_server_uid is None
                    else self.config.expected_server_uid
                )
                if _peer_euid(connection) != expected_uid:
                    raise LabHighWaterAuthorityError(
                        "high-water authority peer identity is not trusted"
                    )
                connection.sendall(line)
                with suppress(OSError):
                    connection.shutdown(socket.SHUT_WR)
                payload = _receive_bounded(connection, _MAX_RESPONSE_BYTES)
        except LabHighWaterAuthorityError:
            raise
        except OSError as exc:
            raise LabHighWaterAuthorityError(
                "high-water authority is unreachable or timed out"
            ) from exc
        if payload is None:
            raise LabHighWaterAuthorityError("high-water authority returned no response")
        try:
            response = strict_model_validate_json(LabHighWaterResponse, payload)
        except Exception as exc:
            raise LabHighWaterAuthorityError("high-water authority response is invalid") from exc
        response.verify(self.config.trusted_authority_key_provider)
        if response.request_id != request_id:
            raise LabHighWaterAuthorityError("high-water authority response is not fresh")
        return response


__all__ = [
    "HIGH_WATER_GENESIS_HASH",
    "GraphReceiptKind",
    "LabHighWaterAdvanceRequest",
    "LabHighWaterAuthorityClient",
    "LabHighWaterAuthorityClientConfig",
    "LabHighWaterAuthorityError",
    "LabHighWaterAuthorityServer",
    "LabHighWaterAuthorityServerConfig",
    "LabHighWaterIdentityError",
    "LabHighWaterKey",
    "LabHighWaterKeyProvider",
    "LabHighWaterRecord",
    "LabHighWaterResponse",
    "LabHighWaterRollbackError",
    "LabHighWaterStatusRequest",
    "LabHighWaterTrustedKeyProvider",
]
