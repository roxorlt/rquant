"""Immutable feature-batch spool separating feature producers from strategies."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import secrets
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Annotated

import pandas as pd
from pydantic import Field, StringConstraints, model_validator

from rquant.feature_contracts import FeatureBatchEnvelope
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class FeatureSpoolIntegrityError(RuntimeError):
    pass


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


class FeatureBatchSpool:
    """Single feature publisher with independent durable consumer cursors."""

    def __init__(self, root: Path) -> None:
        self.root = Path(os.path.abspath(root))
        self.batch_root = self.root / "batches"
        self.cursor_root = self.root / "cursors"
        self.current_path = self.root / "current.json"
        self._identity_path = self.root / "source-identity.json"
        self._lock_path = self.root / ".feature-spool.lock"
        self._thread_lock = RLock()
        self._ensure_private_directories()
        self._source_identity = self._initialize_source_identity()

    def _ensure_private_directories(self) -> None:
        for path in (self.root, self.batch_root, self.cursor_root):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            observed = path.lstat()
            if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid():
                raise FeatureSpoolIntegrityError(f"unsafe feature spool directory: {path}")
            if stat.S_IMODE(observed.st_mode) != 0o700:
                path.chmod(0o700)

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        with self._thread_lock:
            descriptor = os.open(
                self._lock_path,
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
        return json.dumps(
            model.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()

    def _manifest_path(self, sequence: int) -> Path:
        return self.batch_root / f"{sequence:020d}.json"

    def _payload_path(self, sequence: int) -> Path:
        return self.batch_root / f"{sequence:020d}.payload"

    def _cursor_path(self, consumer_id: str) -> Path:
        identity = canonical_sha256({"consumer_id": consumer_id, "spool": "feature/v1"})
        return self.cursor_root / f"{identity}.json"

    def _initialize_source_identity(self) -> _FeatureSourceIdentity:
        with self._exclusive_lock():
            if self._identity_path.exists():
                try:
                    return _FeatureSourceIdentity.model_validate_json(
                        self._identity_path.read_bytes()
                    )
                except (OSError, ValueError) as exc:
                    raise FeatureSpoolIntegrityError("feature source identity is invalid") from exc
            identity = _FeatureSourceIdentity(generation_id=secrets.token_hex(32))
            self._atomic_write(self._identity_path, self._model_bytes(identity))
            return identity

    @staticmethod
    def _validate_payload(envelope: FeatureBatchEnvelope, payload: bytes) -> None:
        if hashlib.sha256(payload).hexdigest() != envelope.content_hash:
            raise FeatureSpoolIntegrityError("payload content hash does not match envelope")
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise FeatureSpoolIntegrityError("feature payload is not valid JSON") from exc
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
        self._validate_payload(envelope, payload)
        manifest_path = self._manifest_path(envelope.sequence)
        payload_path = self._payload_path(envelope.sequence)
        with self._exclusive_lock():
            if manifest_path.exists() or payload_path.exists():
                if not manifest_path.is_file() or not payload_path.is_file():
                    raise FeatureSpoolIntegrityError("immutable batch is only partially present")
                try:
                    existing = FeatureBatchEnvelope.model_validate_json(manifest_path.read_bytes())
                except (OSError, ValueError) as exc:
                    raise FeatureSpoolIntegrityError("existing manifest is invalid") from exc
                if existing != envelope or payload_path.read_bytes() != payload:
                    raise FeatureSpoolIntegrityError(
                        "immutable feature sequence already contains different content"
                    )
                pointer = self._pointer(existing)
                if self.current() is None:
                    sequences = sorted(
                        FeatureBatchEnvelope.model_validate_json(path.read_bytes()).sequence
                        for path in self.batch_root.glob("*.json")
                    )
                    if sequences != list(range(envelope.sequence + 1)):
                        raise FeatureSpoolIntegrityError(
                            "cannot recover current from a non-contiguous latest batch"
                        )
                    self._atomic_write(self.current_path, self._model_bytes(pointer))
                return pointer

            current = self.current()
            expected = 0 if current is None else current.sequence + 1
            if envelope.sequence != expected:
                raise FeatureSpoolIntegrityError(
                    f"next sequence must be {expected}, got {envelope.sequence}"
                )
            self._atomic_write(payload_path, payload)
            self._atomic_write(manifest_path, self._model_bytes(envelope))
            pointer = self._pointer(envelope)
            self._atomic_write(self.current_path, self._model_bytes(pointer))
            return pointer

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
        try:
            pointer = FeatureCurrentPointer.model_validate_json(self.current_path.read_bytes())
        except (OSError, ValueError) as exc:
            raise FeatureSpoolIntegrityError("feature current pointer is invalid") from exc
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
            try:
                envelope = FeatureBatchEnvelope.model_validate_json(path.read_bytes())
            except (OSError, ValueError) as exc:
                raise FeatureSpoolIntegrityError(f"invalid feature manifest: {path.name}") from exc
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
        try:
            payload = record.payload_path.read_bytes()
        except OSError as exc:
            raise FeatureSpoolIntegrityError("feature batch payload is unavailable") from exc
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
        with self._exclusive_lock():
            if cursor.source_generation_id != self._source_identity.generation_id:
                raise FeatureSpoolIntegrityError("feature consumer source generation changed")
            existing = self.load_cursor(cursor.consumer_id)
            if existing is not None and cursor.last_sequence < existing.last_sequence:
                raise FeatureSpoolIntegrityError("feature consumer cursor cannot regress")
            if cursor.last_sequence >= 0:
                path = self._manifest_path(cursor.last_sequence)
                if not path.is_file():
                    raise FeatureSpoolIntegrityError("cursor references a missing batch")
                try:
                    envelope = FeatureBatchEnvelope.model_validate_json(path.read_bytes())
                except (OSError, ValueError) as exc:
                    raise FeatureSpoolIntegrityError("cursor batch manifest is invalid") from exc
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
        try:
            cursor = FeatureConsumerCursor.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise FeatureSpoolIntegrityError("feature consumer cursor is invalid") from exc
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
    "FeatureSpoolIntegrityError",
    "FeatureSourceDescriptor",
    "StoredFeatureResult",
]
