"""Immutable feature-batch spool separating feature producers from strategies."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import secrets
import stat
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from threading import RLock
from typing import Annotated, TypeVar
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, StringConstraints, model_validator

from rquant.feature_contracts import FeatureBatchEnvelope
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_FEATURE_PAYLOAD_BYTES = 128 * 1024 * 1024
_MAX_CONTROL_JSON_BYTES = 1024 * 1024
_MAX_JSON_DEPTH = 64
_MAX_JSON_NODES = 1_000_000
ModelT = TypeVar("ModelT", bound=RuntimeContractModel)


def _preflight_json_depth(
    payload: bytes,
    *,
    label: str,
    maximum_bytes: int = _MAX_FEATURE_PAYLOAD_BYTES,
) -> str:
    if len(payload) > maximum_bytes:
        raise FeatureSpoolIntegrityError(f"{label} exceeds the byte budget")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise FeatureSpoolIntegrityError(f"{label} is not valid UTF-8") from exc
    depth = 0
    nodes = 0
    in_string = False
    escaped = False
    in_atom = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if in_atom:
            if character not in " \t\r\n,]}":
                continue
            in_atom = False
        if character == '"':
            in_string = True
            nodes += 1
        elif character in "[{":
            depth += 1
            nodes += 1
            if depth > _MAX_JSON_DEPTH:
                raise FeatureSpoolIntegrityError(f"{label} exceeds the JSON depth budget")
        elif character in "]}":
            depth -= 1
        elif character not in " \t\r\n,:":
            in_atom = True
            nodes += 1
        if nodes > _MAX_JSON_NODES:
            raise FeatureSpoolIntegrityError(f"{label} exceeds the JSON node budget")
    return text


def _validate_json_node_budget(value: object, *, label: str) -> None:
    stack = [value]
    nodes = 0
    while stack:
        current = stack.pop()
        nodes += 1
        if nodes > _MAX_JSON_NODES:
            raise FeatureSpoolIntegrityError(f"{label} exceeds the JSON node budget")
        if isinstance(current, dict):
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)


class FeatureSpoolIntegrityError(RuntimeError):
    pass


def _read_bounded_file(path: Path, *, label: str, maximum_bytes: int) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise FeatureSpoolIntegrityError(f"{label} is not a regular file")
        if before.st_size > maximum_bytes:
            raise FeatureSpoolIntegrityError(f"{label} exceeds the byte budget")
        with os.fdopen(os.dup(descriptor), "rb") as source:
            payload = source.read(maximum_bytes + 1)
        after = os.fstat(descriptor)
    except FeatureSpoolIntegrityError:
        raise
    except OSError as exc:
        raise FeatureSpoolIntegrityError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if len(payload) > maximum_bytes:
        raise FeatureSpoolIntegrityError(f"{label} exceeds the byte budget")
    if (
        len(payload) != before.st_size
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
        or after.st_ctime_ns != before.st_ctime_ns
    ):
        raise FeatureSpoolIntegrityError(f"{label} changed while it was read")
    return payload


def _canonical_model_bytes(model: RuntimeContractModel) -> bytes:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _read_control_model(path: Path, model: type[ModelT], *, label: str) -> ModelT:
    payload = _read_bounded_file(
        path,
        label=label,
        maximum_bytes=_MAX_CONTROL_JSON_BYTES,
    )
    try:
        decoded = json.loads(
            _preflight_json_depth(
                payload,
                label=label,
                maximum_bytes=_MAX_CONTROL_JSON_BYTES,
            )
        )
        _validate_json_node_budget(decoded, label=label)
        validated = model.model_validate(decoded)
    except FeatureSpoolIntegrityError:
        raise
    except (json.JSONDecodeError, RecursionError, TypeError, ValueError) as exc:
        raise FeatureSpoolIntegrityError(f"{label} is invalid") from exc
    if _canonical_model_bytes(validated) != payload:
        raise FeatureSpoolIntegrityError(f"{label} is not canonical JSON")
    return validated


class FeatureCurrentPointer(RuntimeContractModel):
    source_generation_id: Sha256
    batch_id: str = Field(min_length=1)
    sequence: int = Field(ge=0)
    content_hash: Sha256
    available_at: AwareUtcDatetime


class FeatureConsumerCursor(RuntimeContractModel):
    consumer_id: str = Field(min_length=1)
    source_generation_id: Sha256
    last_sequence: int = Field(ge=-1)
    last_batch_id: str | None = Field(default=None, min_length=1)
    last_content_hash: Sha256 | None = None
    updated_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_identity_pair(self) -> FeatureConsumerCursor:
        has_batch = self.last_batch_id is not None
        has_hash = self.last_content_hash is not None
        if has_batch != has_hash:
            raise ValueError("last batch id and hash must be both set or both absent")
        if self.last_sequence >= 0 and not has_batch:
            raise ValueError("advanced cursor requires batch identity")
        if self.last_sequence == -1 and has_batch:
            raise ValueError("empty cursor cannot contain batch identity")
        return self


@dataclass(frozen=True)
class FeatureBatchRecord:
    envelope: FeatureBatchEnvelope
    manifest_path: Path
    payload_path: Path


@dataclass(frozen=True)
class StoredFeatureResult:
    envelope: FeatureBatchEnvelope
    payload_json: str

    @property
    def frame(self) -> pd.DataFrame:
        payload = json.loads(self.payload_json)
        frame = pd.DataFrame(payload["rows"])
        if "feature_time" in frame:
            frame["feature_time"] = pd.to_datetime(frame["feature_time"], utc=True)
        return frame


class FeatureSourceDescriptor(RuntimeContractModel):
    source_id: str = "feature-spool/global-sequence/v1"
    generation_id: Sha256
    first_sequence: int = 0
    high_watermark: int = Field(ge=-1)


class _FeatureSourceIdentity(RuntimeContractModel):
    generation_id: Sha256


class _FeatureSessionSegmentState(RuntimeContractModel):
    trade_date: date
    source_generation_id: Sha256
    first_sequence: int = Field(ge=0)
    final_sequence: int = Field(ge=0)
    batch_count: int = Field(ge=1)
    segment_chain_hash: Sha256
    final_batch_id: str = Field(min_length=1)
    final_content_hash: Sha256
    final_event_time: AwareUtcDatetime


class FeatureSessionCloseMarker(RuntimeContractModel):
    marker_id: Sha256
    trade_date: date
    session_close_at: AwareUtcDatetime
    source_generation_id: Sha256
    calendar_generation_id: Sha256
    complete_through: AwareUtcDatetime
    upstream_source_generation_id: Sha256
    upstream_final_sequence: int = Field(ge=0)
    upstream_final_batch_id: str = Field(min_length=1)
    upstream_final_content_hash: Sha256
    first_sequence: int = Field(ge=0)
    final_sequence: int = Field(ge=0)
    batch_count: int = Field(ge=1)
    segment_chain_hash: Sha256
    final_batch_id: str = Field(min_length=1)
    final_content_hash: Sha256
    produced_at: AwareUtcDatetime

    @classmethod
    def create(
        cls,
        *,
        trade_date: date,
        session_close_at: datetime,
        source_generation_id: str,
        calendar_generation_id: str,
        complete_through: datetime,
        upstream_source_generation_id: str,
        upstream_final_sequence: int,
        upstream_final_batch_id: str,
        upstream_final_content_hash: str,
        first_sequence: int,
        final_sequence: int,
        batch_count: int,
        segment_chain_hash: str,
        final_batch_id: str,
        final_content_hash: str,
        produced_at: datetime,
    ) -> FeatureSessionCloseMarker:
        identity = {
            "contract": "feature-session-close-marker/v1",
            "trade_date": trade_date,
            "session_close_at": normalize_aware_utc(session_close_at),
            "source_generation_id": source_generation_id,
            "calendar_generation_id": calendar_generation_id,
            "complete_through": normalize_aware_utc(complete_through),
            "upstream_source_generation_id": upstream_source_generation_id,
            "upstream_final_sequence": upstream_final_sequence,
            "upstream_final_batch_id": upstream_final_batch_id,
            "upstream_final_content_hash": upstream_final_content_hash,
            "first_sequence": first_sequence,
            "final_sequence": final_sequence,
            "batch_count": batch_count,
            "segment_chain_hash": segment_chain_hash,
            "final_batch_id": final_batch_id,
            "final_content_hash": final_content_hash,
        }
        return cls(
            marker_id=canonical_sha256(identity),
            produced_at=normalize_aware_utc(produced_at),
            **{key: value for key, value in identity.items() if key != "contract"},
        )

    @model_validator(mode="after")
    def validate_close_marker(self) -> FeatureSessionCloseMarker:
        local_close = self.session_close_at.astimezone(_SHANGHAI)
        if local_close.date() != self.trade_date or local_close.time() != time(15, 0):
            raise ValueError("feature session close marker must bind the 15:00 local close")
        if self.produced_at < self.session_close_at:
            raise ValueError("feature session close marker cannot be produced before close")
        if self.complete_through != self.session_close_at:
            raise ValueError("feature session close marker must cover exactly through close")
        if self.final_sequence < self.first_sequence:
            raise ValueError("feature session sequence range is invalid")
        if self.batch_count != self.final_sequence - self.first_sequence + 1:
            raise ValueError("feature session batch count does not match its range")
        expected = canonical_sha256(self._identity_payload())
        if self.marker_id != expected:
            raise ValueError("feature session close marker identity mismatch")
        return self

    def _identity_payload(self) -> dict[str, object]:
        return {
            "contract": "feature-session-close-marker/v1",
            "trade_date": self.trade_date,
            "session_close_at": self.session_close_at,
            "source_generation_id": self.source_generation_id,
            "calendar_generation_id": self.calendar_generation_id,
            "complete_through": self.complete_through,
            "upstream_source_generation_id": self.upstream_source_generation_id,
            "upstream_final_sequence": self.upstream_final_sequence,
            "upstream_final_batch_id": self.upstream_final_batch_id,
            "upstream_final_content_hash": self.upstream_final_content_hash,
            "first_sequence": self.first_sequence,
            "final_sequence": self.final_sequence,
            "batch_count": self.batch_count,
            "segment_chain_hash": self.segment_chain_hash,
            "final_batch_id": self.final_batch_id,
            "final_content_hash": self.final_content_hash,
        }


class FeatureBatchSpool:
    """Single feature publisher with independent durable consumer cursors."""

    def __init__(
        self,
        root: Path,
        *,
        cursor_root: Path | None = None,
        read_only: bool = False,
    ) -> None:
        self.root = Path(os.path.abspath(root))
        self.read_only = read_only
        self.batch_root = self.root / "batches"
        self.session_root = self.root / "sessions"
        self.cursor_root = Path(
            os.path.abspath(cursor_root if cursor_root is not None else self.root / "cursors")
        )
        self.current_path = self.root / "current.json"
        self._identity_path = self.root / "source-identity.json"
        self._lock_path = self.root / ".feature-spool.lock"
        self._cursor_lock_path = (
            self._lock_path
            if self.cursor_root == self.root / "cursors"
            else self.cursor_root / ".cursor.lock"
        )
        self._thread_lock = RLock()
        self._ensure_private_directories()
        self._source_identity = self._initialize_source_identity()

    def _ensure_private_directories(self) -> None:
        for path in (self.root, self.batch_root, self.session_root):
            if not self.read_only:
                path.mkdir(mode=0o700, parents=True, exist_ok=True)
            elif not path.is_dir():
                raise FeatureSpoolIntegrityError(
                    f"read-only feature spool directory is missing: {path}"
                )
            observed = path.lstat()
            if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid():
                raise FeatureSpoolIntegrityError(f"unsafe feature spool directory: {path}")
            if stat.S_IMODE(observed.st_mode) != 0o700:
                if self.read_only:
                    raise FeatureSpoolIntegrityError(f"unsafe read-only feature spool mode: {path}")
                path.chmod(0o700)
        self.cursor_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        cursor_stat = self.cursor_root.lstat()
        if not stat.S_ISDIR(cursor_stat.st_mode) or cursor_stat.st_uid != os.getuid():
            raise FeatureSpoolIntegrityError(f"unsafe feature cursor directory: {self.cursor_root}")
        if stat.S_IMODE(cursor_stat.st_mode) != 0o700:
            self.cursor_root.chmod(0o700)

    @contextmanager
    def _exclusive_lock(self, path: Path | None = None) -> Iterator[None]:
        with self._thread_lock:
            descriptor = os.open(
                path or self._lock_path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb", closefd=True) as stream:
                descriptor = -1
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
            directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary)

    @staticmethod
    def _model_bytes(model: RuntimeContractModel) -> bytes:
        return _canonical_model_bytes(model)

    def _manifest_path(self, sequence: int) -> Path:
        return self.batch_root / f"{sequence:020d}.json"

    def _payload_path(self, sequence: int) -> Path:
        return self.batch_root / f"{sequence:020d}.payload"

    def _cursor_path(self, consumer_id: str) -> Path:
        identity = canonical_sha256({"consumer_id": consumer_id, "spool": "feature/v1"})
        return self.cursor_root / f"{identity}.json"

    def _session_directory(self, trade_date: date) -> Path:
        return self.session_root / trade_date.isoformat()

    def _session_state_path(self, trade_date: date) -> Path:
        return self._session_directory(trade_date) / "segment.json"

    def _session_marker_path(self, trade_date: date) -> Path:
        return self._session_directory(trade_date) / "close-marker.json"

    @staticmethod
    def _session_trade_date(envelope: FeatureBatchEnvelope) -> date:
        return envelope.event_time.astimezone(_SHANGHAI).date()

    @staticmethod
    def _segment_seed(*, trade_date: date, generation_id: str) -> str:
        return canonical_sha256(
            {
                "contract": "feature-session-segment-chain/v1",
                "trade_date": trade_date,
                "source_generation_id": generation_id,
            }
        )

    @staticmethod
    def _advance_chain(previous: str, envelope: FeatureBatchEnvelope) -> str:
        return canonical_sha256(
            {
                "previous": previous,
                "sequence": envelope.sequence,
                "batch_id": envelope.batch_id,
                "content_hash": envelope.content_hash,
                "envelope_hash": canonical_sha256(envelope.model_dump(mode="json")),
            }
        )

    def _read_session_state(self, trade_date: date) -> _FeatureSessionSegmentState | None:
        path = self._session_state_path(trade_date)
        if not path.exists():
            return None
        state = _read_control_model(
            path,
            _FeatureSessionSegmentState,
            label="feature session segment state",
        )
        if state.trade_date != trade_date:
            raise FeatureSpoolIntegrityError("feature session segment date changed")
        if state.source_generation_id != self._source_identity.generation_id:
            raise FeatureSpoolIntegrityError("feature session segment generation changed")
        return state

    def _read_session_marker(self, trade_date: date) -> FeatureSessionCloseMarker | None:
        path = self._session_marker_path(trade_date)
        if not path.exists():
            return None
        marker = _read_control_model(
            path,
            FeatureSessionCloseMarker,
            label="feature session close marker",
        )
        if marker.trade_date != trade_date:
            raise FeatureSpoolIntegrityError("feature session close marker date changed")
        if marker.source_generation_id != self._source_identity.generation_id:
            raise FeatureSpoolIntegrityError("feature session close marker generation changed")
        state = self._read_session_state(trade_date)
        if state is None or (
            marker.first_sequence != state.first_sequence
            or marker.final_sequence != state.final_sequence
            or marker.batch_count != state.batch_count
            or marker.segment_chain_hash != state.segment_chain_hash
            or marker.final_batch_id != state.final_batch_id
            or marker.final_content_hash != state.final_content_hash
        ):
            raise FeatureSpoolIntegrityError(
                "feature session close marker does not match its frozen segment"
            )
        return marker

    def _append_session_segment(self, envelope: FeatureBatchEnvelope) -> None:
        trade_date = self._session_trade_date(envelope)
        directory = self._session_directory(trade_date)
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        state = self._read_session_state(trade_date)
        if state is None:
            first_sequence = envelope.sequence
            batch_count = 1
            previous = self._segment_seed(
                trade_date=trade_date,
                generation_id=self._source_identity.generation_id,
            )
        else:
            if envelope.sequence != state.final_sequence + 1:
                raise FeatureSpoolIntegrityError(
                    "feature session sequence must advance contiguously"
                )
            if envelope.event_time < state.final_event_time:
                raise FeatureSpoolIntegrityError("feature session event time cannot regress")
            first_sequence = state.first_sequence
            batch_count = state.batch_count + 1
            previous = state.segment_chain_hash
        updated = _FeatureSessionSegmentState(
            trade_date=trade_date,
            source_generation_id=self._source_identity.generation_id,
            first_sequence=first_sequence,
            final_sequence=envelope.sequence,
            batch_count=batch_count,
            segment_chain_hash=self._advance_chain(previous, envelope),
            final_batch_id=envelope.batch_id,
            final_content_hash=envelope.content_hash,
            final_event_time=envelope.event_time,
        )
        self._atomic_write(self._session_state_path(trade_date), self._model_bytes(updated))

    def _initialize_source_identity(self) -> _FeatureSourceIdentity:
        if self.read_only:
            if not self._identity_path.is_file():
                raise FeatureSpoolIntegrityError("read-only feature source identity is missing")
            return _read_control_model(
                self._identity_path,
                _FeatureSourceIdentity,
                label="feature source identity",
            )
        with self._exclusive_lock():
            if self._identity_path.exists():
                return _read_control_model(
                    self._identity_path,
                    _FeatureSourceIdentity,
                    label="feature source identity",
                )
            identity = _FeatureSourceIdentity(generation_id=secrets.token_hex(32))
            self._atomic_write(self._identity_path, self._model_bytes(identity))
            return identity

    @staticmethod
    def _validate_payload(envelope: FeatureBatchEnvelope, payload: bytes) -> None:
        if hashlib.sha256(payload).hexdigest() != envelope.content_hash:
            raise FeatureSpoolIntegrityError("payload content hash does not match envelope")
        try:
            decoded = json.loads(_preflight_json_depth(payload, label="feature payload"))
        except (json.JSONDecodeError, RecursionError) as exc:
            raise FeatureSpoolIntegrityError("feature payload is not valid JSON") from exc
        _validate_json_node_budget(decoded, label="feature payload")
        if not isinstance(decoded, dict):
            raise FeatureSpoolIntegrityError("feature payload must be a JSON object")
        if decoded.get("schema_version") != envelope.schema_version:
            raise FeatureSpoolIntegrityError("feature payload schema_version does not match")
        rows = decoded.get("rows")
        if not isinstance(rows, list) or len(rows) != envelope.row_count:
            raise FeatureSpoolIntegrityError("feature payload row_count does not match")
        canonical = json.dumps(
            decoded,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if canonical != payload:
            raise FeatureSpoolIntegrityError("feature payload is not canonical JSON")

    def publish(self, envelope: FeatureBatchEnvelope, payload: bytes) -> FeatureCurrentPointer:
        if self.read_only:
            raise FeatureSpoolIntegrityError("read-only feature spool cannot publish")
        self._validate_payload(envelope, payload)
        manifest_path = self._manifest_path(envelope.sequence)
        payload_path = self._payload_path(envelope.sequence)
        with self._exclusive_lock():
            if manifest_path.exists() or payload_path.exists():
                if not manifest_path.is_file() or not payload_path.is_file():
                    raise FeatureSpoolIntegrityError("immutable batch is only partially present")
                existing = _read_control_model(
                    manifest_path,
                    FeatureBatchEnvelope,
                    label="existing feature manifest",
                )
                existing_payload = _read_bounded_file(
                    payload_path,
                    label="existing feature payload",
                    maximum_bytes=_MAX_FEATURE_PAYLOAD_BYTES,
                )
                if existing != envelope or existing_payload != payload:
                    raise FeatureSpoolIntegrityError(
                        "immutable feature sequence already contains different content"
                    )
                self._read_session_marker(self._session_trade_date(existing))
                pointer = self._pointer(existing)
                if self.current() is None:
                    sequences = sorted(
                        _read_control_model(
                            path,
                            FeatureBatchEnvelope,
                            label="feature manifest",
                        ).sequence
                        for path in self.batch_root.glob("*.json")
                    )
                    if sequences != list(range(envelope.sequence + 1)):
                        raise FeatureSpoolIntegrityError(
                            "cannot recover current from a non-contiguous latest batch"
                        )
                    self._atomic_write(self.current_path, self._model_bytes(pointer))
                return pointer

            trade_date = self._session_trade_date(envelope)
            if self._read_session_marker(trade_date) is not None:
                raise FeatureSpoolIntegrityError(
                    f"feature session {trade_date.isoformat()} is already closed"
                )
            current = self.current()
            expected = 0 if current is None else current.sequence + 1
            if envelope.sequence != expected:
                raise FeatureSpoolIntegrityError(
                    f"next sequence must be {expected}, got {envelope.sequence}"
                )
            self._atomic_write(payload_path, payload)
            self._atomic_write(manifest_path, self._model_bytes(envelope))
            self._append_session_segment(envelope)
            pointer = self._pointer(envelope)
            self._atomic_write(self.current_path, self._model_bytes(pointer))
            return pointer

    def session_close_marker(self, trade_date: date) -> FeatureSessionCloseMarker | None:
        return self._read_session_marker(trade_date)

    def publish_session_close_marker(
        self,
        *,
        trade_date: date,
        session_close_at: datetime,
        produced_at: datetime,
        calendar_generation_id: str,
        complete_through: datetime,
        upstream_source_generation_id: str,
        upstream_final_sequence: int,
        upstream_final_batch_id: str,
        upstream_final_content_hash: str,
        fault_hook: Callable[[str], None] | None = None,
    ) -> FeatureSessionCloseMarker:
        if self.read_only:
            raise FeatureSpoolIntegrityError(
                "read-only feature spool cannot publish a session close marker"
            )
        close = normalize_aware_utc(session_close_at)
        produced = normalize_aware_utc(produced_at)
        completed = normalize_aware_utc(complete_through)
        with self._exclusive_lock():
            existing = self._read_session_marker(trade_date)
            if existing is not None:
                requested_identity = (
                    close,
                    calendar_generation_id,
                    completed,
                    upstream_source_generation_id,
                    upstream_final_sequence,
                    upstream_final_batch_id,
                    upstream_final_content_hash,
                )
                existing_identity = (
                    existing.session_close_at,
                    existing.calendar_generation_id,
                    existing.complete_through,
                    existing.upstream_source_generation_id,
                    existing.upstream_final_sequence,
                    existing.upstream_final_batch_id,
                    existing.upstream_final_content_hash,
                )
                if existing_identity != requested_identity:
                    raise FeatureSpoolIntegrityError(
                        "feature session close marker conflicts with durable identity"
                    )
                return existing
            state = self._read_session_state(trade_date)
            if state is None:
                raise FeatureSpoolIntegrityError("feature session has no batches to close")
            local_close = close.astimezone(_SHANGHAI)
            if local_close.date() != trade_date or local_close.time() != time(15, 0):
                raise FeatureSpoolIntegrityError(
                    "feature session marker must bind the authoritative 15:00 close"
                )
            if produced < close:
                raise FeatureSpoolIntegrityError(
                    "feature session marker cannot be produced before close"
                )
            if completed != close:
                raise FeatureSpoolIntegrityError(
                    "feature session completion must cover exactly through close"
                )
            if state.final_event_time != close:
                raise FeatureSpoolIntegrityError(
                    "feature session final event must equal the exact 15:00 close"
                )
            marker = FeatureSessionCloseMarker.create(
                trade_date=trade_date,
                session_close_at=close,
                source_generation_id=state.source_generation_id,
                calendar_generation_id=calendar_generation_id,
                complete_through=completed,
                upstream_source_generation_id=upstream_source_generation_id,
                upstream_final_sequence=upstream_final_sequence,
                upstream_final_batch_id=upstream_final_batch_id,
                upstream_final_content_hash=upstream_final_content_hash,
                first_sequence=state.first_sequence,
                final_sequence=state.final_sequence,
                batch_count=state.batch_count,
                segment_chain_hash=state.segment_chain_hash,
                final_batch_id=state.final_batch_id,
                final_content_hash=state.final_content_hash,
                produced_at=produced,
            )
            if fault_hook is not None:
                fault_hook("before_session_close_marker_commit")
            path = self._session_marker_path(trade_date)
            self._atomic_write(path, self._model_bytes(marker))
            if fault_hook is not None:
                fault_hook("after_session_close_marker_commit")
            return marker

    def _pointer(self, envelope: FeatureBatchEnvelope) -> FeatureCurrentPointer:
        return FeatureCurrentPointer(
            source_generation_id=self._source_identity.generation_id,
            batch_id=envelope.batch_id,
            sequence=envelope.sequence,
            content_hash=envelope.content_hash,
            available_at=envelope.available_at,
        )

    def current(self) -> FeatureCurrentPointer | None:
        if not self.current_path.exists():
            return None
        pointer = _read_control_model(
            self.current_path,
            FeatureCurrentPointer,
            label="feature current pointer",
        )
        if pointer.source_generation_id != self._source_identity.generation_id:
            raise FeatureSpoolIntegrityError("feature current pointer generation changed")
        return pointer

    def source_descriptor(self) -> FeatureSourceDescriptor:
        current = self.current()
        return FeatureSourceDescriptor(
            generation_id=self._source_identity.generation_id,
            high_watermark=-1 if current is None else current.sequence,
        )

    def list_after(
        self,
        *,
        sequence: int,
        through_sequence: int | None = None,
        limit: int | None = None,
    ) -> tuple[FeatureBatchRecord, ...]:
        if sequence < -1:
            raise ValueError("sequence cannot be less than -1")
        if through_sequence is not None and through_sequence < sequence:
            raise ValueError("through_sequence cannot precede sequence")
        if limit is not None and limit < 1:
            raise ValueError("limit must be positive")
        current = self.current()
        if current is None:
            if any(self.batch_root.glob("*.json")):
                raise FeatureSpoolIntegrityError("feature current pointer is missing")
            return ()
        through = current.sequence if through_sequence is None else through_sequence
        if through > current.sequence:
            raise FeatureSpoolIntegrityError(
                "requested high watermark exceeds feature source high watermark"
            )
        read_through = through if limit is None else min(through, sequence + limit)
        records: list[FeatureBatchRecord] = []
        for path in sorted(self.batch_root.glob("*.json")):
            envelope = _read_control_model(
                path,
                FeatureBatchEnvelope,
                label=f"feature manifest {path.name}",
            )
            if sequence < envelope.sequence <= read_through:
                records.append(
                    FeatureBatchRecord(
                        envelope=envelope,
                        manifest_path=path,
                        payload_path=self._payload_path(envelope.sequence),
                    )
                )
        if sequence >= read_through:
            return tuple(records)
        expected = list(range(max(sequence + 1, 0), read_through + 1))
        observed = [record.envelope.sequence for record in records]
        if observed != expected:
            raise FeatureSpoolIntegrityError(
                f"feature sequence gap: expected {expected}, observed {observed}"
            )
        return tuple(records)

    def read_payload(self, record: FeatureBatchRecord) -> bytes:
        payload = _read_bounded_file(
            record.payload_path,
            label="feature batch payload",
            maximum_bytes=_MAX_FEATURE_PAYLOAD_BYTES,
        )
        self._validate_payload(record.envelope, payload)
        return payload

    def read_result(self, record: FeatureBatchRecord) -> StoredFeatureResult:
        payload = self.read_payload(record)
        try:
            payload_json = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise FeatureSpoolIntegrityError("feature payload is not UTF-8") from exc
        return StoredFeatureResult(envelope=record.envelope, payload_json=payload_json)

    def commit_cursor(self, cursor: FeatureConsumerCursor) -> None:
        with self._exclusive_lock(self._cursor_lock_path):
            if cursor.source_generation_id != self._source_identity.generation_id:
                raise FeatureSpoolIntegrityError("feature consumer source generation changed")
            existing = self.load_cursor(cursor.consumer_id)
            if existing is not None and cursor.last_sequence < existing.last_sequence:
                raise FeatureSpoolIntegrityError("feature consumer cursor cannot regress")
            if cursor.last_sequence >= 0:
                path = self._manifest_path(cursor.last_sequence)
                if not path.is_file():
                    raise FeatureSpoolIntegrityError("cursor references a missing batch")
                envelope = _read_control_model(
                    path,
                    FeatureBatchEnvelope,
                    label="cursor batch manifest",
                )
                if (
                    envelope.batch_id != cursor.last_batch_id
                    or envelope.content_hash != cursor.last_content_hash
                ):
                    raise FeatureSpoolIntegrityError("cursor does not match its feature batch")
            self._atomic_write(
                self._cursor_path(cursor.consumer_id),
                self._model_bytes(cursor),
            )

    def load_cursor(self, consumer_id: str) -> FeatureConsumerCursor | None:
        path = self._cursor_path(consumer_id)
        if not path.exists():
            return None
        cursor = _read_control_model(
            path,
            FeatureConsumerCursor,
            label="feature consumer cursor",
        )
        if cursor.consumer_id != consumer_id:
            raise FeatureSpoolIntegrityError("feature consumer identity mismatch")
        if cursor.source_generation_id != self._source_identity.generation_id:
            raise FeatureSpoolIntegrityError("feature consumer source generation changed")
        return cursor


__all__ = [
    "FeatureBatchRecord",
    "FeatureBatchSpool",
    "FeatureConsumerCursor",
    "FeatureCurrentPointer",
    "FeatureSessionCloseMarker",
    "FeatureSpoolIntegrityError",
    "FeatureSourceDescriptor",
    "StoredFeatureResult",
]
