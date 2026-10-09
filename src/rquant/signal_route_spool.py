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
from typing import Literal, Self, TypeAlias

from pydantic import (
    ConfigDict,
    Field,
    StrictInt,
    StrictStr,
    TypeAdapter,
    field_validator,
    model_validator,
)

from rquant.condition_alert_route import ConditionAlertBusRoutedRecord
from rquant.condition_alert_runtime_contracts import ConditionRuntimeModel
from rquant.price_alert_route import PriceAlertBusRoutedRecord
from rquant.price_alert_runtime_contracts import PriceRuntimeModel, PriceSha256
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.signal_bus import (
    LegacySignalWriteActivationError,
    SignalBusIntegrityError,
    SignalBusObservedPrefixReceipt,
    SignalBusRoutedRecord,
    SignalBusSignalRecord,
    SignalBusSourceDescriptor,
    SignalBusSourceSequenceError,
    SignalBusStore,
    SignalRouteReceipt,
    require_legacy_signal_write,
)
from rquant.signal_contracts import CurrentSignalEnvelope, current_signal_envelope_json_bytes
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_canonical_json_loads

_SCHEMA_VERSION = 2
_MAX_METADATA_BYTES = 64 * 1024
_MAX_RECORD_BYTES = 4 * 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_CURRENT_SCHEMA_VERSION = 3
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_UTC_DATETIME = TypeAdapter(datetime)


class SignalRouteSpoolIntegrityError(RuntimeError):
    """The immutable routed-signal stream is missing, changed, or unsafe."""


def _current_bytes(value: RuntimeContractModel) -> bytes:
    return canonical_json_bytes(value.model_dump(mode="json"))


def _require_exact_instance(value: object, expected: type[object], *, field: str) -> object:
    if type(value) is not expected:
        raise TypeError(f"{field} requires an exact {expected.__name__} object")
    return value


class CurrentSignalBusRoutedRecord(RuntimeContractModel):
    """Strict future routed record, available only to the Phase-A decoder."""

    model_config = ConfigDict(str_strip_whitespace=False)

    global_sequence: StrictInt = Field(ge=1)
    signal_id: StrictStr = Field(pattern=_SHA256_PATTERN)
    envelope_hash: StrictStr = Field(pattern=_SHA256_PATTERN)
    payload_json: StrictStr = Field(min_length=1)
    envelope: CurrentSignalEnvelope
    received_at: AwareUtcDatetime
    receipt: SignalRouteReceipt

    @model_validator(mode="before")
    @classmethod
    def reject_substituted_nested_models(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise TypeError("current routed record must be an object")
        if "envelope" in value:
            _require_exact_instance(
                value["envelope"],
                CurrentSignalEnvelope,
                field="envelope",
            )
        if "receipt" in value:
            _require_exact_instance(
                value["receipt"],
                SignalRouteReceipt,
                field="receipt",
            )
        return value

    @field_validator("envelope")
    @classmethod
    def validate_exact_envelope(cls, value: CurrentSignalEnvelope) -> CurrentSignalEnvelope:
        return _require_exact_instance(
            value,
            CurrentSignalEnvelope,
            field="envelope",
        )  # type: ignore[return-value]

    @field_validator("receipt")
    @classmethod
    def validate_exact_receipt(cls, value: SignalRouteReceipt) -> SignalRouteReceipt:
        return _require_exact_instance(
            value,
            SignalRouteReceipt,
            field="receipt",
        )  # type: ignore[return-value]

    @field_validator("received_at", mode="before")
    @classmethod
    def validate_utc_received_at(cls, value: datetime) -> datetime:
        if type(value) is not datetime:
            raise TypeError("received_at requires an exact datetime object")
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("received_at must be UTC")
        return normalize_aware_utc(value)

    @model_validator(mode="after")
    def validate_current_identity(self) -> Self:
        envelope_bytes = current_signal_envelope_json_bytes(self.envelope)
        if self.signal_id != self.envelope.signal_id or self.signal_id != self.receipt.signal_id:
            raise ValueError("current routed record signal identity does not match")
        if self.payload_json.encode("utf-8") != envelope_bytes:
            raise ValueError("current routed record payload_json is not exact envelope bytes")
        if self.envelope_hash != _sha256_bytes(envelope_bytes):
            raise ValueError("current routed record envelope hash does not match envelope bytes")
        return self


class CurrentSignalRouteSpoolRecord(RuntimeContractModel):
    """Strict future v3 outer record; deliberately has no publication constructor."""

    model_config = ConfigDict(str_strip_whitespace=False)

    schema_version: StrictInt
    global_sequence: StrictInt = Field(ge=1)
    previous_record_hash: StrictStr | None = Field(pattern=_SHA256_PATTERN)
    envelope_hash: StrictStr = Field(pattern=_SHA256_PATTERN)
    routed_record_hash: StrictStr = Field(pattern=_SHA256_PATTERN)
    record_hash: StrictStr = Field(pattern=_SHA256_PATTERN)
    record: CurrentSignalBusRoutedRecord

    @model_validator(mode="before")
    @classmethod
    def reject_substituted_record(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise TypeError("current route spool record must be an object")
        if "record" in value:
            _require_exact_instance(
                value["record"],
                CurrentSignalBusRoutedRecord,
                field="record",
            )
        return value

    @field_validator("record")
    @classmethod
    def validate_exact_record(
        cls,
        value: CurrentSignalBusRoutedRecord,
    ) -> CurrentSignalBusRoutedRecord:
        return _require_exact_instance(
            value,
            CurrentSignalBusRoutedRecord,
            field="record",
        )  # type: ignore[return-value]

    @model_validator(mode="after")
    def validate_current_hashes(self) -> Self:
        if self.schema_version != _CURRENT_SCHEMA_VERSION:
            raise ValueError("unsupported current routed-signal record schema")
        if self.global_sequence != self.record.global_sequence:
            raise ValueError("current routed-signal wrapper sequence does not match payload")
        if self.envelope_hash != self.record.envelope_hash:
            raise ValueError("current routed-signal wrapper envelope hash does not match payload")
        if self.routed_record_hash != _sha256_bytes(
            current_signal_bus_routed_record_json_bytes(self.record)
        ):
            raise ValueError("current routed-signal record hash does not match payload")
        preimage = canonical_json_bytes(self.model_dump(mode="json", exclude={"record_hash"}))
        if self.record_hash != _sha256_bytes(preimage):
            raise ValueError("current routed-signal outer record hash does not match")
        return self


# Frozen v2 primitive: `ensure_ascii=True` is part of the frozen v2 byte contract and is not
# the current-family `rquant.strict_json.canonical_json_bytes` definition. Do not "unify" them.
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


class SignalBusSpoolPrefixReceipt(RuntimeContractModel):
    """A bus prefix and verified routed spool at the bus observation cutoff."""

    bus_prefix: SignalBusObservedPrefixReceipt
    spool_last_record_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    routed_rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_head(self) -> Self:
        if (self.bus_prefix.source_high_watermark == 0) != (self.spool_last_record_hash is None):
            raise ValueError("empty bus prefix and spool head disagree")
        return self


def _routed_prefix_digest(records: tuple[SignalBusRoutedRecord, ...]) -> str:
    return canonical_sha256({"contract": "signal-bus-routed-prefix/v1", "records": records})


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


# Frozen v2 primitive. `docs/architecture/production-interpreter-authority.md` freezes the
# behavior and the bytes of this function, and `RESET-R07-P2-01` freezes a *separate* future
# v3-only primitive for the later writer tranche. The two v3-only obligations below are NOT
# obligations of this function and must never be retrofitted into it:
#
#   * observing an existing byte-identical immutable target withdraws durability evidence
#     until the records directory is fsynced again (v3-only); this function accepts the
#     existing link and returns without a second directory fsync, and
#   * a byte conflict appends conflict audit evidence before rejecting (v3-only); this
#     function raises `SignalRouteSpoolIntegrityError` and records nothing.
#
# Those two gaps are the frozen *observed* v2 semantics, not a v2 compliance claim. They are
# pinned side by side with the v3 contract as the `frozen-v2-observed` and `v3-spec` dialects
# of `tests/support/signal_route_spool_crash_matrix.py`, exercised from
# `tests/unit/test_signal_route_spool_r07_v3.py`. That model is a synthetic in-memory state
# machine: it is Phase A evidence about the frozen contract, never evidence about a real
# durable current-family writer, because Phase A has none.
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


def _parse_bus_prefix_link(payload: bytes) -> SignalBusSpoolPrefixReceipt:
    try:
        receipt = SignalBusSpoolPrefixReceipt.model_validate_json(payload)
    except ValueError as exc:
        raise SignalRouteSpoolIntegrityError("bus and spool prefix link is invalid") from exc
    if _canonical_bytes(receipt) != payload:
        raise SignalRouteSpoolIntegrityError("bus and spool prefix link is not canonical")
    return receipt


def _parse_record(payload: bytes, *, sequence: int) -> SignalRouteSpoolRecord:
    try:
        return SignalRouteSpoolRecord.model_validate_json(payload)
    except ValueError as exc:
        raise SignalRouteSpoolIntegrityError(
            f"routed-signal record hash or payload is invalid: {sequence}"
        ) from exc


R07RouteSpoolRecord: TypeAlias = SignalRouteSpoolRecord | CurrentSignalRouteSpoolRecord


def current_signal_bus_routed_record_json_bytes(record: CurrentSignalBusRoutedRecord) -> bytes:
    """Return the exact R preimage bytes for a decoded current routed record."""

    _require_exact_instance(record, CurrentSignalBusRoutedRecord, field="record")
    validated = CurrentSignalBusRoutedRecord.model_validate(record)
    return _current_bytes(validated)


def current_signal_route_spool_record_json_bytes(record: CurrentSignalRouteSpoolRecord) -> bytes:
    """Return the exact durable v3 bytes for a decoded current outer record."""

    _require_exact_instance(record, CurrentSignalRouteSpoolRecord, field="record")
    validated = CurrentSignalRouteSpoolRecord.model_validate(record)
    return _current_bytes(validated)


def _decode_current_routed_record(value: object) -> CurrentSignalBusRoutedRecord:
    if not isinstance(value, dict):
        raise TypeError("current routed record must be a JSON object")
    envelope_value = value.get("envelope")
    receipt_value = value.get("receipt")
    received_at = value.get("received_at")
    if not isinstance(envelope_value, dict) or not isinstance(receipt_value, dict):
        raise TypeError("current routed record nested models must be JSON objects")
    received = _UTC_DATETIME.validate_python(received_at)
    return CurrentSignalBusRoutedRecord.model_validate(
        {
            **value,
            "envelope": CurrentSignalEnvelope.model_validate(envelope_value),
            "received_at": received,
            "receipt": SignalRouteReceipt.model_validate(receipt_value),
        }
    )


def decode_current_signal_route_spool_record(payload: bytes) -> CurrentSignalRouteSpoolRecord:
    """Strictly decode one canonical, duplicate-free, non-self-authenticating v3 record."""

    try:
        decoded = strict_canonical_json_loads(payload)
        if not isinstance(decoded, dict):
            raise TypeError("current route spool record must be a JSON object")
        record_value = decoded.get("record")
        if not isinstance(record_value, dict):
            raise TypeError("current route spool record payload must be a JSON object")
        record = CurrentSignalRouteSpoolRecord.model_validate(
            {
                **decoded,
                "record": _decode_current_routed_record(record_value),
            }
        )
        if current_signal_route_spool_record_json_bytes(record) != payload:
            raise StrictJsonError("current route spool record is not canonical")
        return record
    except (StrictJsonError, TypeError, ValueError) as exc:
        raise SignalRouteSpoolIntegrityError(
            "current routed-signal record hash or payload is invalid"
        ) from exc


def _parse_r07_record(payload: bytes, *, sequence: int) -> R07RouteSpoolRecord:
    """Try the byte-preserved v2 parser before the future strict v3 decoder."""

    try:
        return _parse_record(payload, sequence=sequence)
    except SignalRouteSpoolIntegrityError as legacy_error:
        try:
            return decode_current_signal_route_spool_record(payload)
        except SignalRouteSpoolIntegrityError:
            raise legacy_error from None


def verify_current_signal_route_spool_fixture(
    payloads: tuple[bytes, ...],
    *,
    first_sequence: int = 1,
    allow_isolated_current_fixture: bool = False,
) -> tuple[R07RouteSpoolRecord, ...]:
    """Verify an in-memory R07 fixture; it never opens or mutates a spool."""

    if type(first_sequence) is not int or first_sequence < 1:
        raise ValueError("first_sequence must be a positive native integer")
    if type(allow_isolated_current_fixture) is not bool:
        raise TypeError("allow_isolated_current_fixture must be a bool")
    verified: list[R07RouteSpoolRecord] = []
    previous_hash: str | None = None
    current_started = False
    legacy_count = 0
    for sequence, payload in enumerate(payloads, start=first_sequence):
        if type(payload) is not bytes:
            raise TypeError("fixture record bytes must be exact bytes")
        record = _parse_r07_record(payload, sequence=sequence)
        if record.global_sequence != sequence:
            raise SignalRouteSpoolIntegrityError(f"routed-signal sequence gap at {sequence}")
        if isinstance(record, SignalRouteSpoolRecord):
            if current_started:
                raise SignalRouteSpoolIntegrityError("legacy v2 record follows current v3 record")
            legacy_count += 1
        else:
            if not current_started and legacy_count == 0 and not allow_isolated_current_fixture:
                raise SignalRouteSpoolIntegrityError("production route spool cannot start with v3")
            current_started = True
        if record.previous_record_hash != previous_hash:
            raise SignalRouteSpoolIntegrityError(f"routed-signal hash chain mismatch at {sequence}")
        previous_hash = record.record_hash
        verified.append(record)
    return tuple(verified)


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


def _require_legacy_spool_publish_input(
    *,
    source: SignalBusSourceDescriptor,
    records: tuple[SignalBusRoutedRecord, ...],
) -> None:
    if type(source) is not SignalBusSourceDescriptor:
        raise TypeError("SignalRouteSpool.publish requires a SignalBusSourceDescriptor object")
    if type(records) is not tuple:
        raise TypeError("SignalRouteSpool.publish requires a tuple of routed records")
    for record in records:
        if type(record) is CurrentSignalBusRoutedRecord:
            raise LegacySignalWriteActivationError(
                "SignalRouteSpool.publish is legacy-only in this reader-only release; "
                "current-family writes are not activated"
            )
        if type(record) is not SignalBusRoutedRecord:
            raise TypeError("SignalRouteSpool.publish requires SignalBusRoutedRecord objects")
        require_legacy_signal_write(
            record.signal,
            operation="SignalRouteSpool.publish",
        )


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
        _require_legacy_spool_publish_input(source=source, records=records)
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

    def _published_high_watermark(self, *, source: SignalBusSourceDescriptor) -> int:
        """Read the existing v2 pointer without binding or mutating the spool."""

        _require_legacy_spool_publish_input(source=source, records=())
        root_descriptor = _open_root_directory(self.paths.root)
        try:
            records_descriptor = _open_records_directory(root_descriptor)
            try:
                if not _file_exists_at(root_descriptor, "source.json"):
                    return source.first_global_sequence - 1
                identity, pointer, _ = _load_verified_snapshot(
                    root_descriptor,
                    records_descriptor,
                    reject_unpublished_records=False,
                )
                if identity != source.model_copy(update={"high_watermark": 0}):
                    raise SignalRouteSpoolIntegrityError("route spool source generation changed")
                return pointer.source.high_watermark
            finally:
                os.close(records_descriptor)
        finally:
            os.close(root_descriptor)

    def publish_bus_prefix_link(
        self,
        *,
        bus: SignalBusStore,
        observed_at: datetime,
    ) -> SignalBusSpoolPrefixReceipt | None:
        """Persist a bounded same-cutoff bus to spool link after both are verified."""
        bus_prefix = bus.observed_prefix_receipt(observed_at=observed_at)
        if bus_prefix is None:
            return None
        try:
            bus_source = bus.source_descriptor()
            bus_routed = bus.routed_signals_after_global_sequence(
                after_sequence=0,
                through_sequence=bus_prefix.source_high_watermark,
                limit=max(1, bus_prefix.source_high_watermark),
            )
        except (SignalBusIntegrityError, SignalBusSourceSequenceError, TypeError, ValueError):
            return None
        if not bus_prefix.matches_routed_prefix(bus_source, bus_routed):
            return None
        root_descriptor = _open_root_directory(self.paths.root)
        try:
            records_descriptor = _open_records_directory(root_descriptor)
            try:
                with self._exclusive_lock(root_descriptor):
                    _identity, pointer, entries = _load_verified_snapshot(
                        root_descriptor, records_descriptor
                    )
                    routed = tuple(entry.record for entry in entries)
                    if (
                        not bus_prefix.matches_routed_prefix(pointer.source, routed)
                        or routed != bus_routed
                    ):
                        return None
                    link = SignalBusSpoolPrefixReceipt(
                        bus_prefix=bus_prefix,
                        spool_last_record_hash=pointer.last_record_hash,
                        routed_rows_sha256=_routed_prefix_digest(bus_routed),
                    )
                    payload = _canonical_bytes(link)
                    if _file_exists_at(root_descriptor, "bus-prefix-link.json"):
                        previous = _read_file_at(
                            root_descriptor,
                            "bus-prefix-link.json",
                            label="bus and spool prefix link",
                            max_bytes=_MAX_METADATA_BYTES,
                        )
                        if previous == payload:
                            return link
                    _atomic_replace_at(root_descriptor, "bus-prefix-link.json", payload)
                    return link
            finally:
                os.close(records_descriptor)
        finally:
            os.close(root_descriptor)


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

    def bus_prefix_link(self) -> SignalBusSpoolPrefixReceipt | None:
        """Recheck disk records; a cached reader cannot attest to later file damage."""
        with self._lock:
            root_descriptor = _open_root_directory(self.paths.root)
            try:
                records_descriptor = _open_records_directory(root_descriptor)
                try:
                    identity, pointer, entries = _load_verified_snapshot(
                        root_descriptor, records_descriptor
                    )
                    if not _file_exists_at(root_descriptor, "bus-prefix-link.json"):
                        return None
                    payload = _read_file_at(
                        root_descriptor,
                        "bus-prefix-link.json",
                        label="bus and spool prefix link",
                        max_bytes=_MAX_METADATA_BYTES,
                    )
                    link = _parse_bus_prefix_link(payload)
                    current_identity, current_pointer = _load_spool_metadata(
                        root_descriptor, records_descriptor
                    )
                    if current_identity != identity or current_pointer != pointer:
                        return None
                except (FileNotFoundError, SignalRouteSpoolIntegrityError):
                    return None
                finally:
                    os.close(records_descriptor)
            finally:
                os.close(root_descriptor)
            if link.spool_last_record_hash != pointer.last_record_hash:
                return None
            routed = tuple(entry.record for entry in entries)
            if not link.bus_prefix.matches_routed_prefix(
                pointer.source, routed
            ) or link.routed_rows_sha256 != _routed_prefix_digest(routed):
                return None
            return link

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


class PriceAlertRouteSpoolRecord(PriceRuntimeModel):
    schema_version: Literal[4] = 4
    record_schema: Literal["rquant.price-alert-route-record/v1"] = (
        "rquant.price-alert-route-record/v1"
    )
    global_sequence: StrictInt = Field(ge=1)
    previous_record_hash: PriceSha256 | None = None
    payload_hash: PriceSha256
    record_hash: PriceSha256
    record: PriceAlertBusRoutedRecord

    @field_validator("record", mode="before")
    @classmethod
    def exact_price_record(cls, value: object) -> PriceAlertBusRoutedRecord:
        if isinstance(value, dict):
            return PriceAlertBusRoutedRecord.model_validate_json(canonical_json_bytes(value))
        if type(value) is not PriceAlertBusRoutedRecord:
            raise TypeError("price spool requires an exact committed price routed record")
        return PriceAlertBusRoutedRecord.model_validate(value)

    @model_validator(mode="after")
    def verify_price_chain(self) -> Self:
        if (
            self.record.global_sequence != self.global_sequence
            or self.payload_hash != self.record.sha256
        ):
            raise ValueError("price spool payload hash or sequence differs")
        expected = _sha256_bytes(
            canonical_json_bytes(self.model_dump(mode="json", exclude={"record", "record_hash"}))
        )
        if expected != self.record_hash:
            raise ValueError("price spool chain hash differs")
        return self

    @classmethod
    def create(
        cls, *, record: PriceAlertBusRoutedRecord, previous_record_hash: str | None
    ) -> PriceAlertRouteSpoolRecord:
        if type(record) is not PriceAlertBusRoutedRecord:
            raise TypeError("price spool requires the exact price routed record type")
        body = dict(
            schema_version=4,
            record_schema="rquant.price-alert-route-record/v1",
            global_sequence=record.global_sequence,
            previous_record_hash=previous_record_hash,
            payload_hash=record.sha256,
        )
        return cls(**body, record_hash=_sha256_bytes(canonical_json_bytes(body)), record=record)


class ConditionAlertRouteSpoolRecord(ConditionRuntimeModel):
    schema_version: Literal[5] = 5
    record_schema: Literal["rquant.condition-alert-route-record/v1"] = (
        "rquant.condition-alert-route-record/v1"
    )
    global_sequence: StrictInt = Field(ge=1)
    previous_record_hash: PriceSha256 | None = None
    payload_hash: PriceSha256
    record_hash: PriceSha256
    record: ConditionAlertBusRoutedRecord

    @field_validator("record", mode="before")
    @classmethod
    def exact_condition_record(cls, value: object) -> ConditionAlertBusRoutedRecord:
        if isinstance(value, dict):
            return ConditionAlertBusRoutedRecord.model_validate_json(canonical_json_bytes(value))
        if type(value) is not ConditionAlertBusRoutedRecord:
            raise TypeError("condition spool requires an exact committed condition record")
        return ConditionAlertBusRoutedRecord.model_validate(value)

    @model_validator(mode="after")
    def verify_condition_chain(self) -> Self:
        from rquant.condition_alert_runtime_contracts import ConditionAlertEventEnvelope
        from rquant.monitor_builtin_contracts import BuiltinConditionAlertEventEnvelope

        expected_event = ConditionAlertEventEnvelope if self.schema_version == 5 else BuiltinConditionAlertEventEnvelope
        if type(self.record.event) is not expected_event:
            raise ValueError("condition spool variant differs from the exact frozen event codec")
        if (
            self.record.global_sequence != self.global_sequence
            or self.payload_hash != self.record.sha256
        ):
            raise ValueError("condition spool payload hash or sequence differs")
        expected = _sha256_bytes(
            canonical_json_bytes(self.model_dump(mode="json", exclude={"record", "record_hash"}))
        )
        if expected != self.record_hash:
            raise ValueError("condition spool chain hash differs")
        return self

    @classmethod
    def create(
        cls, *, record: ConditionAlertBusRoutedRecord, previous_record_hash: str | None
    ) -> ConditionAlertRouteSpoolRecord:
        if type(record) is not ConditionAlertBusRoutedRecord:
            raise TypeError("condition spool requires the exact condition routed record type")
        record = ConditionAlertBusRoutedRecord.model_validate_json(record.wire_bytes())
        body = dict(
            schema_version=5,
            record_schema="rquant.condition-alert-route-record/v1",
            global_sequence=record.global_sequence,
            previous_record_hash=previous_record_hash,
            payload_hash=record.sha256,
        )
        return cls(**body, record_hash=_sha256_bytes(canonical_json_bytes(body)), record=record)


class BuiltinConditionAlertRouteSpoolRecord(ConditionAlertRouteSpoolRecord):
    schema_version: Literal[6] = 6
    record_schema: Literal["rquant.builtin-condition-alert-route-record/v1"] = "rquant.builtin-condition-alert-route-record/v1"

    @classmethod
    def create(
        cls, *, record: ConditionAlertBusRoutedRecord, previous_record_hash: str | None
    ) -> BuiltinConditionAlertRouteSpoolRecord:
        from rquant.monitor_builtin_contracts import BuiltinConditionAlertEventEnvelope

        if type(record) is not ConditionAlertBusRoutedRecord or type(record.event) is not BuiltinConditionAlertEventEnvelope:
            raise TypeError("builtin spool requires the exact new committed builtin variant")
        record = ConditionAlertBusRoutedRecord.model_validate_json(record.wire_bytes())
        body = dict(schema_version=6, record_schema="rquant.builtin-condition-alert-route-record/v1",
            global_sequence=record.global_sequence, previous_record_hash=previous_record_hash, payload_hash=record.sha256)
        return cls(**body, record_hash=_sha256_bytes(canonical_json_bytes(body)), record=record)


NotificationRouteSpoolRecord: TypeAlias = (
    SignalRouteSpoolRecord | PriceAlertRouteSpoolRecord | ConditionAlertRouteSpoolRecord | BuiltinConditionAlertRouteSpoolRecord
)
NotificationBusRoutedRecord: TypeAlias = (
    SignalBusRoutedRecord | PriceAlertBusRoutedRecord | ConditionAlertBusRoutedRecord
)


class NotificationEventObservedPrefixReceipt(PriceRuntimeModel):
    source_generation_id: PriceSha256
    source_high_watermark: StrictInt = Field(ge=0)
    prefix_row_count: StrictInt = Field(ge=0)
    prefix_rows_sha256: PriceSha256
    source_inspected_at: AwareUtcDatetime
    upstream_complete: Literal[False] = False

    @model_validator(mode="after")
    def complete_durable_prefix(self) -> Self:
        if self.prefix_row_count != self.source_high_watermark:
            raise ValueError("mixed durable prefix count differs from the actual pointer")
        return self


class NotificationEventRouteSpoolPublishSummary(PriceRuntimeModel):
    source_generation_id: PriceSha256
    source_high_watermark: StrictInt = Field(ge=0)
    published_high_watermark: StrictInt = Field(ge=0)
    published_count: StrictInt = Field(ge=0, le=100)
    upstream_complete: Literal[False] = False


def _decode_notification_spool_record(
    payload: bytes, *, sequence: int
) -> NotificationRouteSpoolRecord:
    from rquant.strict_json import strict_json_loads

    body = strict_json_loads(payload)
    if not isinstance(body, dict):
        raise ValueError("mixed routed record is not an object")
    if body.get("schema_version") == 2:
        entry = _parse_record(payload, sequence=sequence)
        if (
            type(entry) is not SignalRouteSpoolRecord
            or type(entry.record) is not SignalBusRoutedRecord
        ):
            raise TypeError("mixed history requires the exact original legacy wrapper")
        require_legacy_signal_write(entry.record.signal, operation="mixed history legacy record")
        if _canonical_bytes(entry) != payload:
            raise ValueError("mixed history legacy bytes differ from the frozen v2 codec")
    elif body.get("schema_version") == 4:
        strict_canonical_json_loads(payload)
        entry = PriceAlertRouteSpoolRecord.model_validate_json(payload)
        if entry.wire_bytes() != payload:
            raise ValueError("price spool record is not canonical")
    elif body.get("schema_version") == 5:
        strict_canonical_json_loads(payload)
        entry = ConditionAlertRouteSpoolRecord.model_validate_json(payload)
        if entry.wire_bytes() != payload:
            raise ValueError("condition spool record is not canonical")
    elif body.get("schema_version") == 6:
        strict_canonical_json_loads(payload)
        entry = BuiltinConditionAlertRouteSpoolRecord.model_validate_json(payload)
        if entry.wire_bytes() != payload:
            raise ValueError("builtin spool record is not canonical")
    else:
        raise TypeError("mixed notification history rejects current v3 or unknown schemas")
    if entry.global_sequence != sequence:
        raise ValueError("mixed routed record sequence differs")
    return entry


class ReadonlyNotificationEventRouteSpool:
    """The same pointer/chain, with a thin verified hash index and bounded record reads."""

    def __init__(self, root: Path, *, _allow_unpublished: bool = False) -> None:
        self.paths = _SignalRouteSpoolPaths(root)
        self._lock = RLock()
        self._identity: SignalBusSourceDescriptor | None = None
        self._pointer: SignalRouteSpoolPointer | None = None
        self._hashes: list[str] = []
        self._latest_time = datetime(1970, 1, 1, tzinfo=UTC)
        self._allow_unpublished = _allow_unpublished

    def _refresh(self, root_descriptor: int, records_descriptor: int) -> SignalRouteSpoolPointer:
        identity, pointer = _load_spool_metadata(
            root_descriptor,
            records_descriptor,
            reject_unpublished_records=not self._allow_unpublished,
        )
        if self._identity is not None and identity != self._identity:
            raise ValueError("mixed notification spool source generation changed")
        previous_high = 0 if self._pointer is None else self._pointer.source.high_watermark
        if pointer.source.high_watermark < previous_high:
            raise ValueError("mixed notification spool pointer regressed")
        previous_hash = None if self._pointer is None else self._pointer.last_record_hash
        hashes = []
        latest_time = self._latest_time
        for sequence in range(previous_high + 1, pointer.source.high_watermark + 1):
            entry = _decode_notification_spool_record(
                _read_file_at(
                    records_descriptor,
                    self.paths.record_name(sequence),
                    label="mixed routed record",
                    max_bytes=_MAX_RECORD_BYTES,
                ),
                sequence=sequence,
            )
            if entry.previous_record_hash != previous_hash:
                raise ValueError("mixed notification spool hash chain differs")
            if (
                type(entry) in {PriceAlertRouteSpoolRecord, ConditionAlertRouteSpoolRecord, BuiltinConditionAlertRouteSpoolRecord}
                and entry.record.bus_generation_id != identity.generation_id
            ):
                raise ValueError("price spool proof belongs to another actual bus")
            available = (
                entry.record.event.available_at
                if type(entry) in {PriceAlertRouteSpoolRecord, ConditionAlertRouteSpoolRecord, BuiltinConditionAlertRouteSpoolRecord}
                else entry.record.signal.available_at
            )
            latest_time = max(
                latest_time, available, entry.record.received_at, entry.record.receipt.routed_at
            )
            hashes.append(entry.record_hash)
            previous_hash = entry.record_hash
        if previous_hash != pointer.last_record_hash:
            raise ValueError("mixed notification spool pointer head differs")
        self._identity, self._pointer = identity, pointer
        self._hashes.extend(hashes)
        self._latest_time = latest_time
        return pointer

    def source_descriptor(self) -> SignalBusSourceDescriptor:
        with self._lock:
            root_descriptor = _open_root_directory(self.paths.root)
            try:
                records_descriptor = _open_records_directory(root_descriptor)
                try:
                    return self._refresh(root_descriptor, records_descriptor).source
                finally:
                    os.close(records_descriptor)
            finally:
                os.close(root_descriptor)

    def routed_after_global_sequence(
        self,
        *,
        after_sequence: int,
        through_sequence: int,
        limit: int,
        observed_at: datetime | None = None,
    ) -> tuple[NotificationBusRoutedRecord, ...]:
        if (
            type(after_sequence) is not int
            or after_sequence < 0
            or type(through_sequence) is not int
            or through_sequence < after_sequence
            or type(limit) is not int
            or not 1 <= limit <= 100
        ):
            raise ValueError("mixed notification spool read range exceeds the route budget")
        cutoff = _utc_now() if observed_at is None else normalize_aware_utc(observed_at)
        with self._lock:
            root_descriptor = _open_root_directory(self.paths.root)
            try:
                records_descriptor = _open_records_directory(root_descriptor)
                try:
                    pointer = self._refresh(root_descriptor, records_descriptor)
                    if through_sequence > pointer.source.high_watermark:
                        raise ValueError(
                            "mixed notification requested watermark exceeds the actual pointer"
                        )
                    output = []
                    for sequence in range(
                        after_sequence + 1, min(through_sequence, after_sequence + limit) + 1
                    ):
                        entry = _decode_notification_spool_record(
                            _read_file_at(
                                records_descriptor,
                                self.paths.record_name(sequence),
                                label="mixed routed record",
                                max_bytes=_MAX_RECORD_BYTES,
                            ),
                            sequence=sequence,
                        )
                        expected_previous = None if sequence == 1 else self._hashes[sequence - 2]
                        if (
                            entry.record_hash != self._hashes[sequence - 1]
                            or entry.previous_record_hash != expected_previous
                        ):
                            raise ValueError("mixed immutable record changed after inspection")
                        record = entry.record
                        available = (
                            record.event.available_at
                            if type(record)
                            in {PriceAlertBusRoutedRecord, ConditionAlertBusRoutedRecord}
                            else record.signal.available_at
                        )
                        if max(available, record.received_at, record.receipt.routed_at) > cutoff:
                            break
                        output.append(record)
                    return tuple(output)
                finally:
                    os.close(records_descriptor)
            finally:
                os.close(root_descriptor)

    def observed_prefix_receipt(
        self, *, observed_at: datetime
    ) -> NotificationEventObservedPrefixReceipt | None:
        inspected = normalize_aware_utc(observed_at)
        with self._lock:
            source = self.source_descriptor()
            if self._latest_time > inspected:
                return None
            return NotificationEventObservedPrefixReceipt(
                source_generation_id=source.generation_id,
                source_high_watermark=source.high_watermark,
                prefix_row_count=len(self._hashes),
                prefix_rows_sha256=_sha256_bytes(canonical_json_bytes(self._hashes)),
                source_inspected_at=inspected,
            )

    def notification_events_after_global_sequence(
        self, *, after_sequence: int, through_sequence: int, observed_at: datetime, limit: int
    ) -> tuple:
        from rquant.condition_alert_route import ConditionAlertBusEventRecord
        from rquant.price_alert_route import PriceAlertBusEventRecord

        records = self.routed_after_global_sequence(
            after_sequence=after_sequence,
            through_sequence=through_sequence,
            observed_at=observed_at,
            limit=limit,
        )
        output = []
        for record in records:
            body = record.model_dump(mode="json", exclude={"receipt"})
            if type(record) is PriceAlertBusRoutedRecord:
                output.append(
                    PriceAlertBusEventRecord.model_validate_json(canonical_json_bytes(body))
                )
            elif type(record) is ConditionAlertBusRoutedRecord:
                output.append(
                    ConditionAlertBusEventRecord.model_validate_json(canonical_json_bytes(body))
                )
            else:
                output.append(
                    SignalBusSignalRecord.model_validate_json(_canonical_object_bytes(body))
                )
        return tuple(output)


def publish_mixed_notification_bus_prefix(
    *, bus: SignalBusStore, spool: SignalRouteSpool, limit: int, observed_at: datetime
) -> NotificationEventRouteSpoolPublishSummary:
    if type(bus) is not SignalBusStore or type(spool) is not SignalRouteSpool:
        raise TypeError("mixed prefix publisher requires the actual original bus and spool types")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("mixed prefix publication exceeds 100 events")
    source = bus.source_descriptor()
    root_descriptor = _open_root_directory(spool.paths.root)
    try:
        records_descriptor = _open_records_directory(root_descriptor)
        try:
            with spool._exclusive_lock(root_descriptor):
                identity = source.model_copy(update={"high_watermark": 0})
                if not _file_exists_at(root_descriptor, "source.json"):
                    _immutable_write_at(
                        root_descriptor,
                        "source.json",
                        _canonical_bytes(identity),
                        label="mixed bus source",
                        max_bytes=_MAX_METADATA_BYTES,
                    )
                reader = getattr(spool, "_notification_event_reader", None)
                if reader is None:
                    reader = ReadonlyNotificationEventRouteSpool(
                        spool.paths.root, _allow_unpublished=True
                    )
                    spool._notification_event_reader = reader
                pointer = reader._refresh(root_descriptor, records_descriptor)
                if (
                    pointer.source.model_copy(update={"high_watermark": 0}) != identity
                    or source.high_watermark < pointer.source.high_watermark
                ):
                    raise ValueError("mixed prefix actual bus source changed or regressed")
                records = bus.routed_notification_events_after_global_sequence(
                    after_sequence=pointer.source.high_watermark,
                    through_sequence=source.high_watermark,
                    observed_at=observed_at,
                    limit=limit,
                )
                previous_hash = pointer.last_record_hash
                prepared: list[tuple[int, bytes]] = []
                cutoff = normalize_aware_utc(observed_at)
                if len(records) > limit:
                    raise ValueError("mixed prefix exceeds its requested batch limit")
                for offset, record in enumerate(records, start=1):
                    if type(record) not in {
                        SignalBusRoutedRecord,
                        PriceAlertBusRoutedRecord,
                        ConditionAlertBusRoutedRecord,
                    }:
                        raise TypeError("mixed prefix cannot write a substituted or unknown record")
                    if (
                        record.global_sequence != pointer.source.high_watermark + offset
                        or record.global_sequence > source.high_watermark
                    ):
                        raise ValueError("mixed prefix cannot skip or exceed its original source")
                    if type(record) is SignalBusRoutedRecord:
                        record = SignalBusRoutedRecord.model_validate_json(_canonical_bytes(record))
                        require_legacy_signal_write(
                            record.signal, operation="mixed committed legacy relay"
                        )
                        entry = SignalRouteSpoolRecord.create(
                            record=record, previous_record_hash=previous_hash
                        )
                        payload = _canonical_bytes(entry)
                    elif type(record) is PriceAlertBusRoutedRecord:
                        if record.bus_generation_id != source.generation_id:
                            raise ValueError("price routed record belongs to another actual bus")
                        entry = PriceAlertRouteSpoolRecord.create(
                            record=record, previous_record_hash=previous_hash
                        )
                        payload = entry.wire_bytes()
                    elif type(record) is ConditionAlertBusRoutedRecord:
                        if record.bus_generation_id != source.generation_id:
                            raise ValueError(
                                "condition routed record belongs to another actual bus"
                            )
                        from rquant.monitor_builtin_contracts import BuiltinConditionAlertEventEnvelope

                        wrapper = BuiltinConditionAlertRouteSpoolRecord if type(record.event) is BuiltinConditionAlertEventEnvelope else ConditionAlertRouteSpoolRecord
                        entry = wrapper.create(
                            record=record, previous_record_hash=previous_hash
                        )
                        payload = entry.wire_bytes()
                    else:
                        raise TypeError("mixed prefix cannot write a substituted or current record")
                    available = (
                        record.signal.available_at
                        if type(record) is SignalBusRoutedRecord
                        else record.event.available_at
                    )
                    if max(available, record.received_at, record.receipt.routed_at) > cutoff:
                        raise ValueError("mixed prefix contains a future event or route receipt")
                    prepared.append((record.global_sequence, payload))
                    previous_hash = entry.record_hash
                for sequence, payload in prepared:
                    _immutable_write_at(
                        records_descriptor,
                        spool.paths.record_name(sequence),
                        payload,
                        label="mixed immutable routed record",
                        max_bytes=_MAX_RECORD_BYTES,
                    )
                high = pointer.source.high_watermark + len(records)
                updated = SignalRouteSpoolPointer(
                    source=source.model_copy(update={"high_watermark": high}),
                    last_record_hash=previous_hash,
                )
                if updated != pointer:
                    _atomic_replace_at(root_descriptor, "current.json", _canonical_bytes(updated))
                return NotificationEventRouteSpoolPublishSummary(
                    source_generation_id=source.generation_id,
                    source_high_watermark=source.high_watermark,
                    published_high_watermark=high,
                    published_count=len(records),
                )
        finally:
            os.close(records_descriptor)
    finally:
        os.close(root_descriptor)


def publish_signal_bus_prefix(
    *,
    bus: SignalBusStore,
    spool: SignalRouteSpool,
    limit: int,
) -> SignalRouteSpoolPublishSummary:
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("limit must be a positive integer")
    source = bus.source_descriptor()
    published_high_watermark = spool._published_high_watermark(source=source)
    records = bus.routed_signals_after_global_sequence(
        after_sequence=published_high_watermark,
        through_sequence=source.high_watermark,
        limit=limit,
    )
    _require_legacy_spool_publish_input(source=source, records=records)
    pointer = spool.publish(source=source, records=())
    if pointer.source.high_watermark != published_high_watermark:
        raise SignalRouteSpoolIntegrityError("route spool pointer changed before publication")
    updated = spool.publish(source=source, records=records)
    return SignalRouteSpoolPublishSummary(
        source_generation_id=source.generation_id,
        source_high_watermark=source.high_watermark,
        published_high_watermark=updated.source.high_watermark,
        published_count=len(records),
    )


__all__ = [
    "SignalBusSpoolPrefixReceipt",
    "CurrentSignalBusRoutedRecord",
    "CurrentSignalRouteSpoolRecord",
    "ReadonlySignalRouteSpool",
    "SignalRouteSpool",
    "SignalRouteSpoolIntegrityError",
    "SignalRouteSpoolPointer",
    "SignalRouteSpoolPublishSummary",
    "SignalRouteSpoolRecord",
    "current_signal_bus_routed_record_json_bytes",
    "current_signal_route_spool_record_json_bytes",
    "decode_current_signal_route_spool_record",
    "publish_signal_bus_prefix",
    "verify_current_signal_route_spool_fixture",
]
