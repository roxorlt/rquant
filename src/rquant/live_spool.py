"""Immutable ordered payload spool for live feed producers and consumers."""

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

from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    ConsumerCursor,
    CurrentPointer,
    LiveChannel,
    LiveSourceDescriptor,
)
from rquant.runtime_contracts import canonical_sha256


class LiveSpoolIntegrityError(RuntimeError):
    pass


@dataclass(frozen=True)
class LiveBatchRecord:
    envelope: BatchEnvelope
    manifest_path: Path
    payload_path: Path


class LiveBatchSpool:
    """A single-producer spool with immutable batches and per-consumer cursors."""

    def __init__(self, root: Path) -> None:
        self.root = Path(os.path.abspath(root))
        self.batch_root = self.root / "batches"
        self.current_root = self.root / "current"
        self.cursor_root = self.root / "cursors"
        self.source_root = self.root / "sources"
        self._lock_path = self.root / ".spool.lock"
        self._thread_lock = RLock()
        self._ensure_private_directories()

    def _ensure_private_directories(self) -> None:
        for path in (
            self.root,
            self.batch_root,
            self.current_root,
            self.cursor_root,
            self.source_root,
        ):
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            observed = path.lstat()
            if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid():
                raise LiveSpoolIntegrityError(f"unsafe spool directory: {path}")
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
    def _json_bytes(
        model: BatchEnvelope | ConsumerCursor | CurrentPointer | LiveSourceDescriptor,
    ) -> bytes:
        return json.dumps(
            model.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @staticmethod
    def _atomic_write(path: Path, payload: bytes) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        try:
            os.fchmod(descriptor, 0o600)
            stream = os.fdopen(descriptor, "wb", closefd=True)
            descriptor = -1
            with stream:
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

    def _channel_dir(self, channel: LiveChannel) -> Path:
        path = self.batch_root / channel.value
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        return path

    def _manifest_path(self, channel: LiveChannel, sequence: int) -> Path:
        return self._channel_dir(channel) / f"{sequence:020d}.json"

    def _payload_path(self, channel: LiveChannel, sequence: int) -> Path:
        return self._channel_dir(channel) / f"{sequence:020d}.payload"

    def _current_path(self, channel: LiveChannel) -> Path:
        return self.current_root / f"{channel.value}.json"

    def _source_path(self, channel: LiveChannel) -> Path:
        return self.source_root / f"{channel.value}.json"

    def _source_generation(self, channel: LiveChannel) -> str:
        path = self._source_path(channel)
        if not path.exists():
            identity = LiveSourceDescriptor(
                channel=channel,
                generation_id=secrets.token_hex(32),
                high_watermark=-1,
            )
            descriptor = -1
            try:
                descriptor = os.open(
                    path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
            except FileExistsError:
                pass
            else:
                try:
                    with os.fdopen(descriptor, "wb", closefd=True) as stream:
                        descriptor = -1
                        stream.write(self._json_bytes(identity))
                        stream.flush()
                        os.fsync(stream.fileno())
                finally:
                    if descriptor >= 0:
                        os.close(descriptor)
                directory = os.open(
                    path.parent,
                    os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                )
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
        try:
            identity = LiveSourceDescriptor.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("live source identity is invalid") from exc
        if identity.channel is not channel or identity.high_watermark != -1:
            raise LiveSpoolIntegrityError("live source identity does not match channel")
        return identity.generation_id

    def _cursor_path(self, consumer_id: str, channel: LiveChannel) -> Path:
        identity = canonical_sha256({"consumer_id": consumer_id, "channel": channel.value})
        return self.cursor_root / f"{identity}.json"

    def publish(self, envelope: BatchEnvelope, payload: bytes) -> CurrentPointer:
        observed_hash = hashlib.sha256(payload).hexdigest()
        if observed_hash != envelope.content_sha256:
            raise LiveSpoolIntegrityError("payload content hash does not match envelope")
        if envelope.quality_status in {
            BatchQualityStatus.CANDIDATE,
            BatchQualityStatus.QUARANTINED,
        }:
            raise LiveSpoolIntegrityError("batch quality cannot become current")

        source_generation_id = self._source_generation(envelope.channel)
        with self._exclusive_lock():
            manifest_path = self._manifest_path(envelope.channel, envelope.sequence)
            payload_path = self._payload_path(envelope.channel, envelope.sequence)
            if manifest_path.exists() or payload_path.exists():
                return self._validate_idempotent_replay(
                    envelope=envelope,
                    payload=payload,
                    manifest_path=manifest_path,
                    payload_path=payload_path,
                )

            current = self.current(envelope.channel)
            expected_sequence = 0 if current is None else current.sequence + 1
            if envelope.sequence != expected_sequence:
                raise LiveSpoolIntegrityError(
                    f"next sequence must be {expected_sequence}, got {envelope.sequence}"
                )

            self._atomic_write(payload_path, payload)
            self._atomic_write(manifest_path, self._json_bytes(envelope))
            pointer = CurrentPointer(
                channel=envelope.channel,
                source_generation_id=source_generation_id,
                batch_id=envelope.batch_id,
                sequence=envelope.sequence,
                revision=envelope.revision,
                content_sha256=envelope.content_sha256,
                quality_status=envelope.quality_status,
                published_at=envelope.received_at,
            )
            self._atomic_write(self._current_path(envelope.channel), self._json_bytes(pointer))
            return pointer

    def _validate_idempotent_replay(
        self,
        *,
        envelope: BatchEnvelope,
        payload: bytes,
        manifest_path: Path,
        payload_path: Path,
    ) -> CurrentPointer:
        if not manifest_path.is_file() or not payload_path.is_file():
            raise LiveSpoolIntegrityError("immutable batch is only partially present")
        stored = BatchEnvelope.model_validate_json(manifest_path.read_bytes())
        if stored != envelope or payload_path.read_bytes() != payload:
            raise LiveSpoolIntegrityError("immutable sequence already contains different content")
        return CurrentPointer(
            channel=stored.channel,
            source_generation_id=self._source_generation(stored.channel),
            batch_id=stored.batch_id,
            sequence=stored.sequence,
            revision=stored.revision,
            content_sha256=stored.content_sha256,
            quality_status=stored.quality_status,
            published_at=stored.received_at,
        )

    def current(self, channel: LiveChannel) -> CurrentPointer | None:
        path = self._current_path(channel)
        if not path.exists():
            return None
        try:
            pointer = CurrentPointer.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("current pointer is invalid") from exc
        if pointer.source_generation_id != self._source_generation(channel):
            raise LiveSpoolIntegrityError("current pointer source generation changed")
        return pointer

    def source_descriptor(self, channel: LiveChannel) -> LiveSourceDescriptor:
        current = self.current(channel)
        return LiveSourceDescriptor(
            channel=channel,
            generation_id=self._source_generation(channel),
            high_watermark=-1 if current is None else current.sequence,
        )

    def list_after(self, channel: LiveChannel, *, sequence: int) -> tuple[LiveBatchRecord, ...]:
        records: list[LiveBatchRecord] = []
        for path in sorted(self._channel_dir(channel).glob("*.json")):
            try:
                envelope = BatchEnvelope.model_validate_json(path.read_bytes())
            except (OSError, ValueError) as exc:
                raise LiveSpoolIntegrityError(f"invalid batch manifest: {path.name}") from exc
            if envelope.channel is not channel:
                raise LiveSpoolIntegrityError("batch manifest channel does not match its directory")
            if envelope.sequence > sequence:
                records.append(
                    LiveBatchRecord(
                        envelope=envelope,
                        manifest_path=path,
                        payload_path=self._payload_path(channel, envelope.sequence),
                    )
                )
        current = self.current(channel)
        if current is None or sequence >= current.sequence:
            return tuple(records)
        expected = list(range(max(sequence + 1, 0), current.sequence + 1))
        observed = [record.envelope.sequence for record in records]
        if observed != expected:
            raise LiveSpoolIntegrityError(
                f"batch sequence gap: expected {expected}, observed {observed}"
            )
        return tuple(records)

    def read_payload(self, record: LiveBatchRecord) -> bytes:
        try:
            payload = record.payload_path.read_bytes()
        except OSError as exc:
            raise LiveSpoolIntegrityError("batch payload is unavailable") from exc
        if hashlib.sha256(payload).hexdigest() != record.envelope.content_sha256:
            raise LiveSpoolIntegrityError("batch payload content hash mismatch")
        return payload

    def commit_cursor(self, cursor: ConsumerCursor) -> None:
        with self._exclusive_lock():
            if cursor.source_generation_id != self._source_generation(cursor.channel):
                raise LiveSpoolIntegrityError("consumer source generation changed")
            existing = self.load_cursor(cursor.consumer_id, cursor.channel)
            if existing is not None and cursor.last_sequence < existing.last_sequence:
                raise LiveSpoolIntegrityError("consumer cursor cannot regress")
            if cursor.last_sequence >= 0:
                manifest = self._manifest_path(cursor.channel, cursor.last_sequence)
                if not manifest.is_file():
                    raise LiveSpoolIntegrityError("consumer cursor references a missing batch")
                envelope = BatchEnvelope.model_validate_json(manifest.read_bytes())
                if (
                    envelope.batch_id != cursor.last_batch_id
                    or envelope.content_sha256 != cursor.last_content_sha256
                ):
                    raise LiveSpoolIntegrityError("consumer cursor does not match its batch")
            self._atomic_write(
                self._cursor_path(cursor.consumer_id, cursor.channel),
                self._json_bytes(cursor),
            )

    def load_cursor(
        self,
        consumer_id: str,
        channel: LiveChannel,
    ) -> ConsumerCursor | None:
        path = self._cursor_path(consumer_id, channel)
        if not path.exists():
            return None
        try:
            cursor = ConsumerCursor.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("consumer cursor is invalid") from exc
        if cursor.consumer_id != consumer_id or cursor.channel is not channel:
            raise LiveSpoolIntegrityError("consumer cursor identity mismatch")
        if cursor.source_generation_id != self._source_generation(channel):
            raise LiveSpoolIntegrityError("consumer source generation changed")
        return cursor
