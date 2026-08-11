"""Immutable routed-signal spool owned by the single signal-router process."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import secrets
import stat
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Self

from pydantic import Field, model_validator

from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.signal_bus import (
    SignalBusRoutedRecord,
    SignalBusSignalRecord,
    SignalBusSourceDescriptor,
    SignalBusStore,
)

_SCHEMA_VERSION = 2
_MAX_METADATA_BYTES = 64 * 1024
_MAX_RECORD_BYTES = 4 * 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class SignalRouteSpoolIntegrityError(RuntimeError):
    """The immutable routed-signal stream is missing, changed, or unsafe."""


def _canonical_object_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _canonical_bytes(model: RuntimeContractModel) -> bytes:
    return _canonical_object_bytes(model.model_dump(mode="json"))


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _record_chain_hash(
    *,
    global_sequence: int,
    previous_record_hash: str | None,
    payload_hash: str,
) -> str:
    return _sha256_bytes(
        _canonical_object_bytes(
            {
                "global_sequence": global_sequence,
                "payload_hash": payload_hash,
                "previous_record_hash": previous_record_hash,
                "schema_version": _SCHEMA_VERSION,
            }
        )
    )


class SignalRouteSpoolRecord(RuntimeContractModel):
    schema_version: int = Field(default=_SCHEMA_VERSION, ge=_SCHEMA_VERSION)
    global_sequence: int = Field(ge=1)
    previous_record_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    record: SignalBusRoutedRecord

    @model_validator(mode="after")
    def validate_hashes(self) -> Self:
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("unsupported routed-signal record schema")
        if self.record.global_sequence != self.global_sequence:
            raise ValueError("routed-signal wrapper sequence does not match payload")
        expected_payload_hash = _sha256_bytes(_canonical_bytes(self.record))
        if self.payload_hash != expected_payload_hash:
            raise ValueError("routed-signal canonical payload hash mismatch")
        expected_record_hash = _record_chain_hash(
            global_sequence=self.global_sequence,
            previous_record_hash=self.previous_record_hash,
            payload_hash=self.payload_hash,
        )
        if self.record_hash != expected_record_hash:
            raise ValueError("routed-signal record hash mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        record: SignalBusRoutedRecord,
        previous_record_hash: str | None,
    ) -> SignalRouteSpoolRecord:
        payload_hash = _sha256_bytes(_canonical_bytes(record))
        return cls(
            global_sequence=record.global_sequence,
            previous_record_hash=previous_record_hash,
            payload_hash=payload_hash,
            record_hash=_record_chain_hash(
                global_sequence=record.global_sequence,
                previous_record_hash=previous_record_hash,
                payload_hash=payload_hash,
            ),
            record=record,
        )


class SignalRouteSpoolPointer(RuntimeContractModel):
    schema_version: int = Field(default=_SCHEMA_VERSION, ge=_SCHEMA_VERSION)
    source: SignalBusSourceDescriptor
    last_record_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_empty_pointer(self) -> Self:
        if self.schema_version != _SCHEMA_VERSION:
            raise ValueError("unsupported route spool pointer schema")
        empty = self.source.high_watermark < self.source.first_global_sequence
        if empty != (self.last_record_hash is None):
            raise ValueError("empty route spool pointer and head hash disagree")
        return self


class SignalRouteSpoolPublishSummary(RuntimeContractModel):
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_high_watermark: int = Field(ge=0)
    published_high_watermark: int = Field(ge=0)
    published_count: int = Field(ge=0)


class _SignalRouteSpoolPaths:
    def __init__(self, root: Path) -> None:
        self.root = Path(os.path.abspath(root))
        self.records = self.root / "records"

    @staticmethod
    def record_name(sequence: int) -> str:
        return f"{sequence:020d}.json"


def _same_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        left.st_mode,
        left.st_nlink,
        left.st_uid,
        left.st_size,
        left.st_mtime_ns,
        left.st_ctime_ns,
    ) == (
        right.st_dev,
        right.st_ino,
        right.st_mode,
        right.st_nlink,
        right.st_uid,
        right.st_size,
        right.st_mtime_ns,
        right.st_ctime_ns,
    )


def _validate_directory(descriptor: int, *, label: str, require_owner: bool) -> None:
    observed = os.fstat(descriptor)
    if not stat.S_ISDIR(observed.st_mode):
        raise SignalRouteSpoolIntegrityError(f"unsafe route spool directory: {label}")
    if require_owner and observed.st_uid != os.geteuid():
        raise SignalRouteSpoolIntegrityError(f"unsafe route spool directory owner: {label}")


def _open_root_directory(root: Path) -> int:
    absolute = Path(os.path.abspath(root))
    descriptor = os.open(os.path.sep, _DIRECTORY_FLAGS)
    try:
        parts = absolute.parts[1:] if absolute.is_absolute() else absolute.parts
        for part in parts:
            child = os.open(part, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        _validate_directory(descriptor, label=str(absolute), require_owner=True)
        return descriptor
    except (OSError, SignalRouteSpoolIntegrityError) as exc:
        os.close(descriptor)
        if isinstance(exc, SignalRouteSpoolIntegrityError):
            raise
        raise SignalRouteSpoolIntegrityError("route spool is unavailable or unsafe") from exc


def _open_records_directory(root_descriptor: int) -> int:
    try:
        descriptor = os.open("records", _DIRECTORY_FLAGS, dir_fd=root_descriptor)
        _validate_directory(descriptor, label="records", require_owner=True)
        return descriptor
    except (OSError, SignalRouteSpoolIntegrityError) as exc:
        if isinstance(exc, SignalRouteSpoolIntegrityError):
            raise
        raise SignalRouteSpoolIntegrityError(
            "route spool records directory is unavailable or unsafe"
        ) from exc


def _read_file_at(
    directory_descriptor: int,
    name: str,
    *,
    label: str,
    max_bytes: int,
) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(name, _READ_FLAGS, dir_fd=directory_descriptor)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != os.geteuid()
            or before.st_size > max_bytes
        ):
            raise SignalRouteSpoolIntegrityError(f"unsafe {label}")
        remaining = before.st_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, remaining)
            if not chunk:
                raise SignalRouteSpoolIntegrityError(f"{label} changed during read")
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        path_after = os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
        if not _same_identity(before, after) or not _same_identity(after, path_after):
            raise SignalRouteSpoolIntegrityError(f"{label} changed during read")
        payload = b"".join(chunks)
        if len(payload) != before.st_size:
            raise SignalRouteSpoolIntegrityError(f"{label} changed during read")
        return payload
    except FileNotFoundError:
        raise
    except SignalRouteSpoolIntegrityError:
        raise
    except OSError as exc:
        raise SignalRouteSpoolIntegrityError(f"unsafe or unreadable {label}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _file_exists_at(directory_descriptor: int, name: str) -> bool:
    try:
        os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return False
    return True


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        offset += os.write(descriptor, payload[offset:])


def _write_temporary_at(directory_descriptor: int, name: str, payload: bytes) -> str:
    temporary = f".{name}.{secrets.token_hex(16)}"
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
        dir_fd=directory_descriptor,
    )
    try:
        _write_all(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return temporary


def _atomic_replace_at(
    directory_descriptor: int,
    name: str,
    payload: bytes,
) -> None:
    temporary = _write_temporary_at(directory_descriptor, name, payload)
    try:
        os.replace(
            temporary,
            name,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
        )
        os.fsync(directory_descriptor)
    finally:
        with suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=directory_descriptor)


def _immutable_write_at(
    directory_descriptor: int,
    name: str,
    payload: bytes,
    *,
    label: str,
    max_bytes: int,
) -> None:
    if _file_exists_at(directory_descriptor, name):
        if (
            _read_file_at(
                directory_descriptor,
                name,
                label=label,
                max_bytes=max_bytes,
            )
            != payload
        ):
            raise SignalRouteSpoolIntegrityError(f"immutable {label} changed")
        return
    temporary = _write_temporary_at(directory_descriptor, name, payload)
    try:
        try:
            os.link(
                temporary,
                name,
                src_dir_fd=directory_descriptor,
                dst_dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
            os.fsync(directory_descriptor)
        except FileExistsError:
            if (
                _read_file_at(
                    directory_descriptor,
                    name,
                    label=label,
                    max_bytes=max_bytes,
                )
                != payload
            ):
                raise SignalRouteSpoolIntegrityError(f"immutable {label} changed") from None
    finally:
        with suppress(FileNotFoundError):
            os.unlink(temporary, dir_fd=directory_descriptor)


def _parse_source(payload: bytes) -> SignalBusSourceDescriptor:
    try:
        return SignalBusSourceDescriptor.model_validate_json(payload)
    except ValueError as exc:
        raise SignalRouteSpoolIntegrityError("route spool source identity is invalid") from exc


def _parse_pointer(payload: bytes) -> SignalRouteSpoolPointer:
    try:
        return SignalRouteSpoolPointer.model_validate_json(payload)
    except ValueError as exc:
        raise SignalRouteSpoolIntegrityError("route spool current pointer is invalid") from exc


def _parse_record(payload: bytes, *, sequence: int) -> SignalRouteSpoolRecord:
    try:
        return SignalRouteSpoolRecord.model_validate_json(payload)
    except ValueError as exc:
        raise SignalRouteSpoolIntegrityError(
            f"routed-signal record hash or payload is invalid: {sequence}"
        ) from exc


def _validate_source_identity(
    identity: SignalBusSourceDescriptor,
    pointer: SignalRouteSpoolPointer,
) -> None:
    if pointer.source.model_copy(update={"high_watermark": 0}) != identity:
        raise SignalRouteSpoolIntegrityError("route spool generation changed")


def _load_spool_metadata(
    root_descriptor: int,
    records_descriptor: int,
    *,
    reject_unpublished_records: bool = True,
) -> tuple[SignalBusSourceDescriptor, SignalRouteSpoolPointer]:
    try:
        identity = _parse_source(
            _read_file_at(
                root_descriptor,
                "source.json",
                label="route spool source metadata",
                max_bytes=_MAX_METADATA_BYTES,
            )
        )
    except FileNotFoundError as exc:
        raise SignalRouteSpoolIntegrityError("route spool source metadata is missing") from exc

    if not _file_exists_at(root_descriptor, "current.json"):
        has_records = any(
            len(name) == 25 and name[:20].isdigit() and name.endswith(".json")
            for name in os.listdir(records_descriptor)
        )
        if reject_unpublished_records and has_records:
            raise SignalRouteSpoolIntegrityError(
                "route spool current pointer is missing for published records"
            )
        pointer = SignalRouteSpoolPointer(source=identity)
    else:
        try:
            pointer = _parse_pointer(
                _read_file_at(
                    root_descriptor,
                    "current.json",
                    label="route spool current pointer",
                    max_bytes=_MAX_METADATA_BYTES,
                )
            )
        except FileNotFoundError as exc:
            raise SignalRouteSpoolIntegrityError(
                "route spool current pointer changed during read"
            ) from exc
    _validate_source_identity(identity, pointer)
    return identity, pointer


def _load_verified_records(
    records_descriptor: int,
    *,
    first_sequence: int,
    high_watermark: int,
    previous_record_hash: str | None,
) -> tuple[tuple[SignalRouteSpoolRecord, ...], str | None]:
    entries: list[SignalRouteSpoolRecord] = []
    previous_hash = previous_record_hash
    for sequence in range(first_sequence, high_watermark + 1):
        name = _SignalRouteSpoolPaths.record_name(sequence)
        try:
            entry = _parse_record(
                _read_file_at(
                    records_descriptor,
                    name,
                    label=f"routed-signal record {name}",
                    max_bytes=_MAX_RECORD_BYTES,
                ),
                sequence=sequence,
            )
        except FileNotFoundError as exc:
            raise SignalRouteSpoolIntegrityError(
                f"routed-signal sequence is missing: {sequence}"
            ) from exc
        if entry.global_sequence != sequence:
            raise SignalRouteSpoolIntegrityError(f"routed-signal sequence gap at {sequence}")
        if entry.previous_record_hash != previous_hash:
            raise SignalRouteSpoolIntegrityError(f"routed-signal hash chain mismatch at {sequence}")
        previous_hash = entry.record_hash
        entries.append(entry)
    return tuple(entries), previous_hash


def _load_verified_snapshot(
    root_descriptor: int,
    records_descriptor: int,
    *,
    reject_unpublished_records: bool = True,
) -> tuple[
    SignalBusSourceDescriptor,
    SignalRouteSpoolPointer,
    tuple[SignalRouteSpoolRecord, ...],
]:
    identity, pointer = _load_spool_metadata(
        root_descriptor,
        records_descriptor,
        reject_unpublished_records=reject_unpublished_records,
    )

    first = pointer.source.first_global_sequence
    entries, previous_hash = _load_verified_records(
        records_descriptor,
        first_sequence=first,
        high_watermark=pointer.source.high_watermark,
        previous_record_hash=None,
    )
    if previous_hash != pointer.last_record_hash:
        raise SignalRouteSpoolIntegrityError("route spool pointer head hash mismatch")
    return identity, pointer, entries


class SignalRouteSpool:
    """Publish one global routed-signal sequence through atomic immutable files."""

    def __init__(self, root: Path) -> None:
        self.paths = _SignalRouteSpoolPaths(root)
        self.paths.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.paths.records.mkdir(mode=0o700, exist_ok=True)
        root_descriptor = _open_root_directory(self.paths.root)
        try:
            records_descriptor = _open_records_directory(root_descriptor)
            os.close(records_descriptor)
            os.fchmod(root_descriptor, 0o700)
        finally:
            os.close(root_descriptor)
        self.paths.records.chmod(0o700)

    @contextmanager
    def _exclusive_lock(self, root_descriptor: int) -> Iterator[None]:
        descriptor = os.open(
            ".writer.lock",
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_descriptor,
        )
        try:
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_nlink != 1
                or observed.st_uid != os.geteuid()
            ):
                raise SignalRouteSpoolIntegrityError("unsafe route spool writer lock")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            with suppress(OSError):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def publish(
        self,
        *,
        source: SignalBusSourceDescriptor,
        records: tuple[SignalBusRoutedRecord, ...],
    ) -> SignalRouteSpoolPointer:
        root_descriptor = _open_root_directory(self.paths.root)
        try:
            records_descriptor = _open_records_directory(root_descriptor)
            try:
                with self._exclusive_lock(root_descriptor):
                    pointer = self._bind_source(
                        root_descriptor=root_descriptor,
                        records_descriptor=records_descriptor,
                        source=source,
                    )
                    if source.high_watermark < pointer.source.high_watermark:
                        raise SignalRouteSpoolIntegrityError(
                            "route spool source high watermark regressed"
                        )
                    expected = pointer.source.high_watermark + 1
                    last_hash = pointer.last_record_hash
                    for record in records:
                        if record.global_sequence != expected:
                            raise SignalRouteSpoolIntegrityError(
                                f"routed signal sequence gap: expected {expected}, "
                                f"observed {record.global_sequence}"
                            )
                        entry = SignalRouteSpoolRecord.create(
                            record=record,
                            previous_record_hash=last_hash,
                        )
                        name = self.paths.record_name(record.global_sequence)
                        _immutable_write_at(
                            records_descriptor,
                            name,
                            _canonical_bytes(entry),
                            label=f"routed-signal record {name}",
                            max_bytes=_MAX_RECORD_BYTES,
                        )
                        expected += 1
                        last_hash = entry.record_hash
                    high_watermark = expected - 1
                    updated = SignalRouteSpoolPointer(
                        source=source.model_copy(update={"high_watermark": high_watermark}),
                        last_record_hash=last_hash,
                    )
                    if updated != pointer:
                        _atomic_replace_at(
                            root_descriptor,
                            "current.json",
                            _canonical_bytes(updated),
                        )
                    return updated
            finally:
                os.close(records_descriptor)
        finally:
            os.close(root_descriptor)

    @staticmethod
    def _bind_source(
        *,
        root_descriptor: int,
        records_descriptor: int,
        source: SignalBusSourceDescriptor,
    ) -> SignalRouteSpoolPointer:
        identity = source.model_copy(update={"high_watermark": 0})
        if _file_exists_at(root_descriptor, "source.json"):
            try:
                observed_identity = _parse_source(
                    _read_file_at(
                        root_descriptor,
                        "source.json",
                        label="route spool source metadata",
                        max_bytes=_MAX_METADATA_BYTES,
                    )
                )
            except FileNotFoundError as exc:
                raise SignalRouteSpoolIntegrityError(
                    "route spool source metadata changed during read"
                ) from exc
            if observed_identity != identity:
                raise SignalRouteSpoolIntegrityError("route spool source generation changed")
        else:
            _immutable_write_at(
                root_descriptor,
                "source.json",
                _canonical_bytes(identity),
                label="route spool source metadata",
                max_bytes=_MAX_METADATA_BYTES,
            )
        _, pointer, _ = _load_verified_snapshot(
            root_descriptor,
            records_descriptor,
            reject_unpublished_records=False,
        )
        if pointer.source.model_copy(update={"high_watermark": 0}) != identity:
            raise SignalRouteSpoolIntegrityError("route spool source generation changed")
        return pointer


class ReadonlySignalRouteSpool:
    """Read a verified routed-signal prefix without creating files or cursors."""

    def __init__(self, root: Path) -> None:
        self.paths = _SignalRouteSpoolPaths(root)
        self._lock = RLock()
        self._verified_identity: SignalBusSourceDescriptor | None = None
        self._verified_pointer: SignalRouteSpoolPointer | None = None
        self._verified_entries: list[SignalRouteSpoolRecord] = []
        root_descriptor = _open_root_directory(self.paths.root)
        try:
            records_descriptor = _open_records_directory(root_descriptor)
            os.close(records_descriptor)
        finally:
            os.close(root_descriptor)

    def _refresh_locked(self) -> SignalRouteSpoolPointer:
        root_descriptor = _open_root_directory(self.paths.root)
        try:
            records_descriptor = _open_records_directory(root_descriptor)
            try:
                if self._verified_pointer is None:
                    identity, pointer, entries = _load_verified_snapshot(
                        root_descriptor,
                        records_descriptor,
                    )
                    self._verified_identity = identity
                    self._verified_pointer = pointer
                    self._verified_entries.extend(entries)
                    return pointer

                identity, pointer = _load_spool_metadata(
                    root_descriptor,
                    records_descriptor,
                )
                if identity != self._verified_identity:
                    raise SignalRouteSpoolIntegrityError("route spool source generation changed")

                verified_pointer = self._verified_pointer
                verified_high_watermark = verified_pointer.source.high_watermark
                observed_high_watermark = pointer.source.high_watermark
                if observed_high_watermark < verified_high_watermark:
                    raise SignalRouteSpoolIntegrityError(
                        "route spool current pointer high watermark regressed"
                    )
                if observed_high_watermark == verified_high_watermark:
                    if pointer.last_record_hash != verified_pointer.last_record_hash:
                        raise SignalRouteSpoolIntegrityError(
                            "route spool head changed at the current high watermark"
                        )
                    return verified_pointer

                appended, observed_head = _load_verified_records(
                    records_descriptor,
                    first_sequence=verified_high_watermark + 1,
                    high_watermark=observed_high_watermark,
                    previous_record_hash=verified_pointer.last_record_hash,
                )
                if observed_head != pointer.last_record_hash:
                    raise SignalRouteSpoolIntegrityError("route spool pointer head hash mismatch")
                self._verified_entries.extend(appended)
                self._verified_pointer = pointer
                return pointer
            finally:
                os.close(records_descriptor)
        finally:
            os.close(root_descriptor)

    def source_descriptor(self) -> SignalBusSourceDescriptor:
        with self._lock:
            return self._refresh_locked().source

    def routed_after_global_sequence(
        self,
        *,
        after_sequence: int,
        through_sequence: int,
        limit: int,
        observed_at: datetime | None = None,
    ) -> tuple[SignalBusRoutedRecord, ...]:
        if after_sequence < 0 or through_sequence < after_sequence or limit < 1:
            raise ValueError("invalid routed-signal read bounds")
        with self._lock:
            pointer = self._refresh_locked()
            if through_sequence > pointer.source.high_watermark:
                raise SignalRouteSpoolIntegrityError(
                    "requested high watermark exceeds the published route spool"
                )
            first = pointer.source.first_global_sequence
            lower = max(after_sequence + 1, first)
            upper = min(through_sequence, after_sequence + limit)
            if upper < lower:
                entries: tuple[SignalRouteSpoolRecord, ...] = ()
            else:
                start_index = lower - first
                stop_index = upper - first + 1
                entries = tuple(self._verified_entries[start_index:stop_index])
        cutoff = normalize_aware_utc(observed_at) if observed_at is not None else _utc_now()
        visible: list[SignalBusRoutedRecord] = []
        for entry in entries:
            record = entry.record
            if (
                record.signal.available_at > cutoff
                or record.received_at > cutoff
                or record.receipt.routed_at > cutoff
            ):
                break
            visible.append(record)
        return tuple(visible)

    def signals_after_global_sequence(
        self,
        *,
        after_sequence: int,
        through_sequence: int,
        observed_at: datetime,
        limit: int,
    ) -> tuple[SignalBusSignalRecord, ...]:
        return tuple(
            SignalBusSignalRecord.model_validate(
                record.model_dump(mode="python", exclude={"receipt"})
            )
            for record in self.routed_after_global_sequence(
                after_sequence=after_sequence,
                through_sequence=through_sequence,
                observed_at=observed_at,
                limit=limit,
            )
        )


def publish_signal_bus_prefix(
    *,
    bus: SignalBusStore,
    spool: SignalRouteSpool,
    limit: int,
) -> SignalRouteSpoolPublishSummary:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")
    source = bus.source_descriptor()
    pointer = spool.publish(source=source, records=())
    records = bus.routed_signals_after_global_sequence(
        after_sequence=pointer.source.high_watermark,
        through_sequence=source.high_watermark,
        limit=limit,
    )
    updated = spool.publish(source=source, records=records)
    return SignalRouteSpoolPublishSummary(
        source_generation_id=source.generation_id,
        source_high_watermark=source.high_watermark,
        published_high_watermark=updated.source.high_watermark,
        published_count=len(records),
    )


__all__ = [
    "ReadonlySignalRouteSpool",
    "SignalRouteSpool",
    "SignalRouteSpoolIntegrityError",
    "SignalRouteSpoolPointer",
    "SignalRouteSpoolPublishSummary",
    "SignalRouteSpoolRecord",
    "publish_signal_bus_prefix",
]
