"""Immutable ordered payload spool for live feed producers and consumers."""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import secrets
import stat
import subprocess
import tempfile
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import RLock

from pydantic import Field

from rquant.live_contracts import (
    BatchEnvelope,
    BatchPointer,
    BatchQualityStatus,
    ConsumerCursor,
    CurrentPointer,
    LiveChannel,
    LiveSourceDescriptor,
)
from rquant.reference_data_registry import (
    ReferencePublicationAuthenticator,
    ReferencePublicationCommitIntent,
    ReferencePublicationCompletionReceipt,
    ReferencePublicationDurableEvidence,
    reference_publication_commit_intent_path,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads


class LiveSpoolIntegrityError(RuntimeError):
    pass


_SSH_KEYGEN_PATH = Path("/usr/bin/ssh-keygen")
_SSH_KEYGEN_TIMEOUT_SECONDS = 5.0


def _trusted_ssh_keygen_path() -> str:
    if not _SSH_KEYGEN_PATH.is_absolute():
        raise LiveSpoolIntegrityError("ssh-keygen path is not absolute")
    try:
        observed = _SSH_KEYGEN_PATH.lstat()
    except OSError as exc:
        raise LiveSpoolIntegrityError("trusted ssh-keygen is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != 0
        or observed.st_nlink != 1
        or stat.S_IMODE(observed.st_mode) & 0o022
        or not os.access(_SSH_KEYGEN_PATH, os.X_OK)
    ):
        raise LiveSpoolIntegrityError("trusted ssh-keygen binary is unsafe")
    return str(_SSH_KEYGEN_PATH)


def _secure_read_regular_file(
    path: Path,
    *,
    label: str,
    max_bytes: int = 256 * 1024 * 1024,
) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size > max_bytes
        ):
            raise LiveSpoolIntegrityError(f"{label} is unsafe")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        if remaining <= 0 and os.read(descriptor, 1):
            raise LiveSpoolIntegrityError(f"{label} is too large")
        after = os.fstat(descriptor)
        if (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ):
            raise LiveSpoolIntegrityError(f"{label} identity changed while reading")
        return b"".join(chunks)
    except LiveSpoolIntegrityError:
        raise
    except OSError as exc:
        raise LiveSpoolIntegrityError(f"{label} is unavailable or a symlink") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


class ReferenceSourceBatchSigner:
    """Ed25519 signer whose private credential is available only to the source service."""

    _NAMESPACE = "rquant-reference-source"

    def __init__(
        self,
        *,
        key_id: str,
        private_key_path: Path | None = None,
        private_key: str | None = None,
    ) -> None:
        if not key_id or any(character.isspace() for character in key_id):
            raise ValueError("reference source signing key_id is invalid")
        if (private_key_path is None) == (private_key is None):
            raise ValueError("exactly one reference source private credential is required")
        self.key_id = key_id
        self.private_key_path = (
            Path(os.path.abspath(private_key_path)) if private_key_path is not None else None
        )
        self._private_key = private_key.encode("ascii") if private_key is not None else None

    def sign(self, payload: bytes) -> str:
        private_key = self._private_key
        if private_key is None:
            assert self.private_key_path is not None
            private_key = _secure_read_regular_file(
                self.private_key_path,
                label="reference source private signing key",
                max_bytes=64 * 1024,
            )
        with tempfile.TemporaryDirectory(prefix="rquant-source-sign-") as directory_name:
            directory = Path(directory_name)
            directory.chmod(0o700)
            key_path = directory / "key"
            data_path = directory / "payload"
            LiveBatchSpool._atomic_write(key_path, private_key)
            LiveBatchSpool._atomic_write(data_path, payload)
            try:
                completed = subprocess.run(
                    (
                        _trusted_ssh_keygen_path(),
                        "-Y",
                        "sign",
                        "-f",
                        str(key_path),
                        "-n",
                        self._NAMESPACE,
                        str(data_path),
                    ),
                    check=False,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=_SSH_KEYGEN_TIMEOUT_SECONDS,
                )
            except subprocess.TimeoutExpired as exc:
                raise LiveSpoolIntegrityError("reference source batch signing timed out") from exc
            except OSError as exc:
                raise LiveSpoolIntegrityError(
                    "reference source batch signing command failed"
                ) from exc
            signature_path = Path(f"{data_path}.sig")
            if completed.returncode != 0 or not signature_path.is_file():
                raise LiveSpoolIntegrityError("reference source batch signing failed")
            signature_path.chmod(0o600)
            signature = _secure_read_regular_file(
                signature_path,
                label="reference source batch signature",
                max_bytes=64 * 1024,
            )
        return base64.b64encode(signature).decode("ascii")

    @classmethod
    def from_environment(cls) -> ReferenceSourceBatchSigner | None:
        key_id = os.environ.get("RQ_REFERENCE_SOURCE_SIGNING_KEY_ID", "").strip()
        private_key = os.environ.get("RQ_REFERENCE_SOURCE_PRIVATE_KEY", "")
        if not key_id and not private_key:
            return None
        if not key_id or not private_key:
            raise LiveSpoolIntegrityError("reference source signing environment is incomplete")
        return cls(key_id=key_id, private_key=private_key)


class ReferenceSourceBatchVerifier:
    """Verify source signatures using a public Ed25519 credential only."""

    _NAMESPACE = ReferenceSourceBatchSigner._NAMESPACE

    def __init__(self, *, key_id: str, public_key: str) -> None:
        normalized = public_key.strip()
        if (
            not key_id
            or any(character.isspace() for character in key_id)
            or not normalized.startswith("ssh-ed25519 ")
        ):
            raise ValueError("reference source verification credential is invalid")
        self.key_id = key_id
        self.public_key = normalized

    def verify(self, payload: bytes, signature_base64: str) -> bool:
        try:
            signature = base64.b64decode(signature_base64, validate=True)
        except ValueError:
            return False
        with tempfile.TemporaryDirectory(prefix="rquant-source-verify-") as directory_name:
            directory = Path(directory_name)
            directory.chmod(0o700)
            allowed_path = directory / "allowed-signers"
            signature_path = directory / "signature"
            LiveBatchSpool._atomic_write(
                allowed_path,
                f"{self.key_id} {self.public_key}\n".encode("ascii"),
            )
            LiveBatchSpool._atomic_write(signature_path, signature)
            try:
                completed = subprocess.run(
                    (
                        _trusted_ssh_keygen_path(),
                        "-Y",
                        "verify",
                        "-f",
                        str(allowed_path),
                        "-I",
                        self.key_id,
                        "-n",
                        self._NAMESPACE,
                        "-s",
                        str(signature_path),
                    ),
                    input=payload,
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=_SSH_KEYGEN_TIMEOUT_SECONDS,
                )
            except subprocess.TimeoutExpired as exc:
                raise LiveSpoolIntegrityError(
                    "reference source batch verification timed out"
                ) from exc
            except OSError as exc:
                raise LiveSpoolIntegrityError(
                    "reference source batch verification command failed"
                ) from exc
        return completed.returncode == 0

    @classmethod
    def from_environment(cls) -> ReferenceSourceBatchVerifier | None:
        key_id = os.environ.get("RQ_REFERENCE_SOURCE_SIGNING_KEY_ID", "").strip()
        public_key = os.environ.get("RQ_REFERENCE_SOURCE_PUBLIC_KEY", "").strip()
        if not key_id and not public_key:
            return None
        if not key_id or not public_key:
            raise LiveSpoolIntegrityError("reference source verification environment is incomplete")
        return cls(key_id=key_id, public_key=public_key)


@dataclass(frozen=True)
class LiveBatchRecord:
    envelope: BatchEnvelope
    manifest_path: Path
    payload_path: Path


class _SpoolPublicationIntent(RuntimeContractModel):
    schema_version: int = 1
    channel: LiveChannel
    sequence: int = Field(ge=0)
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    envelope_identity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_pointer: CurrentPointer | None = None
    target_pointer: BatchPointer
    advances_current: bool = True
    remove_immutable_batch: bool
    not_after: AwareUtcDatetime | None = None


class _CursorPublicationIntent(RuntimeContractModel):
    schema_version: int = 1
    consumer_id: str = Field(min_length=1)
    channel: LiveChannel
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_cursor: ConsumerCursor | None = None
    target_cursor: ConsumerCursor
    not_after: AwareUtcDatetime | None = None
    publication_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    completion_receipt_path: str | None = None
    registry_generation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class _SpoolCompletionReceipt(RuntimeContractModel):
    schema_version: int = 1
    channel: LiveChannel
    sequence: int = Field(ge=0)
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    pointer_identity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    envelope_identity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    batch_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    quality_status: BatchQualityStatus
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_time: AwareUtcDatetime
    received_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    visible_at: AwareUtcDatetime
    receipt_sha256: str = ""

    @property
    def content_payload(self) -> dict[str, object]:
        return {
            key: value
            for key, value in self.model_dump(mode="python").items()
            if key != "receipt_sha256"
        }

    def model_post_init(self, __context: object) -> None:
        del __context
        expected = canonical_sha256(self.content_payload)
        if self.receipt_sha256 and self.receipt_sha256 != expected:
            raise ValueError("receipt_sha256 does not match spool completion receipt")
        object.__setattr__(self, "receipt_sha256", expected)


class _ReferenceSourceBatchSignature(RuntimeContractModel):
    schema_version: int = 1
    key_id: str = Field(min_length=1, max_length=128)
    channel: LiveChannel
    sequence: int = Field(ge=0)
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    descriptor_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    signature_base64: str = Field(min_length=1)

    def signing_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "contract": "reference-source-batch-signature/v1",
                **self.model_dump(mode="json", exclude={"signature_base64"}),
            }
        )


class ReferenceRetiredBatchDigest(RuntimeContractModel):
    sequence: int = Field(ge=0)
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    publication_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_signature_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ReferenceSpoolRetirementReceipt(RuntimeContractModel):
    schema_version: int = 1
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    consumer_id: str = Field(min_length=1)
    previous_retired_through_sequence: int = Field(ge=-1)
    retired_through_sequence: int = Field(ge=0)
    previous_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    batches: tuple[ReferenceRetiredBatchDigest, ...] = Field(min_length=1, max_length=256)
    retired_at: AwareUtcDatetime
    key_id: str = Field(min_length=1, max_length=128)
    signature_base64: str = Field(min_length=1)

    def signing_payload(self) -> bytes:
        return canonical_json_bytes(
            {
                "contract": "reference-spool-retirement/v1",
                **self.model_dump(mode="json", exclude={"signature_base64"}),
            }
        )


class _ReferenceSpoolRetentionIndex(RuntimeContractModel):
    schema_version: int = 1
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    retired_through_sequence: int = Field(ge=0)
    receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class _ReferenceSpoolRetirementIntent(RuntimeContractModel):
    schema_version: int = 1
    receipt: ReferenceSpoolRetirementReceipt


class LiveBatchSpool:
    """A single-producer spool with immutable batches and per-consumer cursors."""

    def __init__(
        self,
        root: Path,
        *,
        cursor_root: Path | None = None,
        read_only: bool = False,
        source_read_only: bool = False,
        publication_authenticator: ReferencePublicationAuthenticator | None = None,
        source_signer: ReferenceSourceBatchSigner | None = None,
        source_verifier: ReferenceSourceBatchVerifier | None = None,
    ) -> None:
        self.root = Path(os.path.abspath(root))
        self.read_only = read_only
        self.source_read_only = read_only or source_read_only
        self.publication_authenticator = (
            publication_authenticator or ReferencePublicationAuthenticator.from_environment()
        )
        self.source_signer = source_signer or ReferenceSourceBatchSigner.from_environment()
        self.source_verifier = source_verifier or ReferenceSourceBatchVerifier.from_environment()
        self.batch_root = self.root / "batches"
        self.current_root = self.root / "current"
        self.intent_root = self.root / "publication-intents"
        self.publication_receipt_root = self.root / "publication-receipts"
        self.source_signature_root = self.root / "source-signatures"
        self.reference_archive_root = self.root / "archive" / LiveChannel.REFERENCE_SLOW.value
        self.reference_retirement_root = self.root / "retirement" / LiveChannel.REFERENCE_SLOW.value
        self.reference_retirement_receipt_root = self.reference_retirement_root / "receipts"
        self.reference_retirement_index_path = self.reference_retirement_root / "index.json"
        self.reference_retirement_intent_path = self.reference_retirement_root / "intent.json"
        self.cursor_root = Path(
            os.path.abspath(cursor_root if cursor_root is not None else self.root / "cursors")
        )
        self.cursor_intent_root = self.cursor_root / "publication-intents"
        self.completion_receipt_root = self.cursor_root / "completion-receipts"
        self.completion_evidence_root = self.cursor_root / "completion-evidence"
        self.source_root = self.root / "sources"
        self._lock_path = self.root / ".spool.lock"
        self._cursor_lock_path = (
            self._lock_path
            if self.cursor_root == self.root / "cursors"
            else self.cursor_root / ".cursor.lock"
        )
        self._reference_publication_lock_path = (
            self.cursor_root / ".reference-publication.commit.lock"
        )
        self._thread_lock = RLock()
        self._ensure_private_directories()

    def _ensure_private_directories(self) -> None:
        source_paths = (
            self.root,
            self.batch_root,
            self.current_root,
            self.intent_root,
            self.publication_receipt_root,
            self.source_signature_root,
            self.source_root,
            self.reference_archive_root,
            self.reference_archive_root / "manifests",
            self.reference_archive_root / "payloads",
            self.reference_archive_root / "publication-receipts",
            self.reference_archive_root / "source-signatures",
            self.reference_retirement_root,
            self.reference_retirement_receipt_root,
        )
        for path in source_paths:
            if not self.source_read_only:
                path.mkdir(mode=0o700, parents=True, exist_ok=True)
            elif not path.exists():
                if path == self.root:
                    raise LiveSpoolIntegrityError(f"read-only spool directory is missing: {path}")
                continue
            observed = path.lstat()
            if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid():
                raise LiveSpoolIntegrityError(f"unsafe spool directory: {path}")
            if stat.S_IMODE(observed.st_mode) != 0o700:
                if self.source_read_only:
                    raise LiveSpoolIntegrityError(f"unsafe read-only spool mode: {path}")
                path.chmod(0o700)
        cursor_paths = (
            (self.cursor_root, "cursor"),
            (self.cursor_intent_root, "cursor intent"),
            (self.completion_receipt_root, "completion receipt"),
            (self.completion_evidence_root, "completion evidence"),
        )
        if self.read_only and not self.cursor_root.exists():
            return
        for path, label in cursor_paths:
            if self.read_only and not path.exists():
                continue
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
            observed = path.lstat()
            if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid():
                raise LiveSpoolIntegrityError(f"unsafe {label} directory: {path}")
            if stat.S_IMODE(observed.st_mode) != 0o700:
                if self.read_only:
                    raise LiveSpoolIntegrityError(f"unsafe read-only {label} mode: {path}")
                path.chmod(0o700)

    def _require_cursor_writer(self) -> None:
        if self.read_only:
            raise LiveSpoolIntegrityError("read-only spool cannot modify cursor state")

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

    @contextmanager
    def reference_publication_commit_lock(self) -> Iterator[None]:
        """Serialize the registry/cursor/receipt commit protocol under one lock."""

        self._require_cursor_writer()
        with self._exclusive_lock(self._reference_publication_lock_path):
            yield

    @staticmethod
    def _json_bytes(
        model: RuntimeContractModel,
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

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _channel_dir(self, channel: LiveChannel) -> Path:
        path = self.batch_root / channel.value
        if not self.source_read_only:
            path.mkdir(mode=0o700, parents=True, exist_ok=True)
        elif path.exists():
            observed = path.lstat()
            if (
                not stat.S_ISDIR(observed.st_mode)
                or observed.st_uid != os.getuid()
                or stat.S_IMODE(observed.st_mode) != 0o700
            ):
                raise LiveSpoolIntegrityError("read-only channel directory is unsafe")
        return path

    def _manifest_path(self, channel: LiveChannel, sequence: int) -> Path:
        return self._channel_dir(channel) / f"{sequence:020d}.json"

    def _payload_path(self, channel: LiveChannel, sequence: int) -> Path:
        return self._channel_dir(channel) / f"{sequence:020d}.payload"

    def _current_path(self, channel: LiveChannel) -> Path:
        return self.current_root / f"{channel.value}.json"

    def _intent_path(self, channel: LiveChannel) -> Path:
        return self.intent_root / f"{channel.value}.json"

    def _publication_receipt_path(
        self,
        channel: LiveChannel,
        sequence: int,
    ) -> Path:
        return self.publication_receipt_root / channel.value / f"{sequence:020d}.json"

    def _source_signature_path(self, channel: LiveChannel, sequence: int) -> Path:
        return self.source_signature_root / channel.value / f"{sequence:020d}.json"

    def reference_archive_paths(self, sequence: int) -> tuple[Path, Path, Path, Path]:
        if sequence < 0:
            raise ValueError("reference archive sequence must be non-negative")
        name = f"{sequence:020d}"
        return (
            self.reference_archive_root / "manifests" / f"{name}.json",
            self.reference_archive_root / "payloads" / f"{name}.payload",
            self.reference_archive_root / "publication-receipts" / f"{name}.json",
            self.reference_archive_root / "source-signatures" / f"{name}.json",
        )

    def reference_retirement_receipt_path(self, retired_through_sequence: int) -> Path:
        if retired_through_sequence < 0:
            raise ValueError("retirement receipt sequence must be non-negative")
        return self.reference_retirement_receipt_root / f"{retired_through_sequence:020d}.json"

    def _source_path(self, channel: LiveChannel) -> Path:
        return self.source_root / f"{channel.value}.json"

    def load_source_state(self, name: str) -> bytes | None:
        if not name or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for character in name
        ):
            raise ValueError("source state name is invalid")
        path = self.source_root / f"{name}.state.json"
        if not path.exists():
            return None
        return _secure_read_regular_file(
            path,
            label="source state",
            max_bytes=1024 * 1024,
        )

    def store_source_state(self, name: str, payload: bytes) -> None:
        if self.source_read_only:
            raise LiveSpoolIntegrityError("source read-only spool cannot update source state")
        if not name or any(
            character not in "abcdefghijklmnopqrstuvwxyz0123456789-_" for character in name
        ):
            raise ValueError("source state name is invalid")
        self._atomic_write(self.source_root / f"{name}.state.json", payload)

    def _source_generation(self, channel: LiveChannel) -> str:
        path = self._source_path(channel)
        if not path.exists():
            if self.source_read_only:
                raise LiveSpoolIntegrityError("read-only spool source identity is missing")
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
            identity = LiveSourceDescriptor.model_validate_json(
                _secure_read_regular_file(
                    path,
                    label="live source descriptor",
                    max_bytes=64 * 1024,
                )
            )
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("live source identity is invalid") from exc
        if identity.channel is not channel or identity.high_watermark != -1:
            raise LiveSpoolIntegrityError("live source identity does not match channel")
        return identity.generation_id

    def _cursor_path(self, consumer_id: str, channel: LiveChannel) -> Path:
        identity = canonical_sha256({"consumer_id": consumer_id, "channel": channel.value})
        return self.cursor_root / f"{identity}.json"

    def _cursor_intent_path(self, consumer_id: str, channel: LiveChannel) -> Path:
        identity = canonical_sha256({"consumer_id": consumer_id, "channel": channel.value})
        return self.cursor_intent_root / f"{identity}.json"

    def completion_receipt_path(self, publication_id: str) -> Path:
        if len(publication_id) != 64 or any(
            character not in "0123456789abcdef" for character in publication_id
        ):
            raise ValueError("publication_id must be lowercase sha256")
        return self.completion_receipt_root / f"{publication_id}.json"

    def completion_receipt_intent_path(self, publication_id: str) -> Path:
        return reference_publication_commit_intent_path(
            self.completion_receipt_path(publication_id)
        )

    def _load_publication_intent(
        self,
        channel: LiveChannel,
    ) -> _SpoolPublicationIntent | None:
        path = self._intent_path(channel)
        if not path.exists():
            return None
        try:
            intent = _SpoolPublicationIntent.model_validate_json(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("publication intent is invalid") from exc
        if intent.channel is not channel:
            raise LiveSpoolIntegrityError("publication intent channel does not match its path")
        if intent.source_generation_id != self._source_generation(channel):
            raise LiveSpoolIntegrityError("publication intent source generation changed")
        return intent

    def _clear_publication_intent(self, channel: LiveChannel) -> None:
        path = self._intent_path(channel)
        with suppress(FileNotFoundError):
            path.unlink()
        directory = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _recover_publication_intent_locked(self, channel: LiveChannel) -> None:
        intent = self._load_publication_intent(channel)
        if intent is None:
            return
        if self.source_read_only:
            raise LiveSpoolIntegrityError("pending publication intent requires writer recovery")
        manifest_path = self._manifest_path(channel, intent.sequence)
        payload_path = self._payload_path(channel, intent.sequence)
        receipt_path = self._publication_receipt_path(channel, intent.sequence)
        signature_path = self._source_signature_path(channel, intent.sequence)
        current_path = self._current_path(channel)
        observed_pointer: CurrentPointer | None = None
        if current_path.exists():
            try:
                observed_pointer = CurrentPointer.model_validate_json(current_path.read_bytes())
            except (OSError, ValueError) as exc:
                raise LiveSpoolIntegrityError(
                    "publication intent found an invalid current pointer"
                ) from exc
        allowed_pointers = {intent.previous_pointer}
        if intent.advances_current:
            allowed_pointers.add(intent.target_pointer)
        if manifest_path.is_file():
            try:
                stored = BatchEnvelope.model_validate_json(manifest_path.read_bytes())
            except (OSError, ValueError) as exc:
                raise LiveSpoolIntegrityError(
                    "publication intent found an invalid target manifest"
                ) from exc
            stored_pointer = BatchPointer(
                channel=stored.channel,
                source_generation_id=intent.source_generation_id,
                batch_id=stored.batch_id,
                sequence=stored.sequence,
                revision=stored.revision,
                content_sha256=stored.content_sha256,
                quality_status=stored.quality_status,
                published_at=stored.available_at,
            )
            if intent.advances_current:
                allowed_pointers.add(CurrentPointer.model_validate(stored_pointer.model_dump()))
            elif (
                stored.identity_sha256 != intent.envelope_identity_sha256
                or stored_pointer != intent.target_pointer
                or not payload_path.is_file()
            ):
                raise LiveSpoolIntegrityError("non-current publication intent is invalid")
        if observed_pointer not in allowed_pointers:
            raise LiveSpoolIntegrityError("publication intent current pointer is not recoverable")
        if not intent.advances_current and manifest_path.is_file() and payload_path.is_file():
            try:
                payload = _secure_read_regular_file(
                    payload_path,
                    label="non-current immutable batch payload",
                )
            except OSError as exc:
                raise LiveSpoolIntegrityError(
                    "non-current publication intent payload is invalid"
                ) from exc
            if hashlib.sha256(payload).hexdigest() != intent.target_pointer.content_sha256:
                raise LiveSpoolIntegrityError("non-current publication intent payload is corrupt")
            if channel is LiveChannel.REFERENCE_SLOW and receipt_path.exists():
                try:
                    envelope = BatchEnvelope.model_validate_json(manifest_path.read_bytes())
                except (OSError, ValueError) as exc:
                    raise LiveSpoolIntegrityError(
                        "reference non-current publication intent manifest is invalid"
                    ) from exc
                self._validate_publication_receipt(
                    envelope,
                    source_generation_id=intent.source_generation_id,
                )
                self._clear_publication_intent(channel)
                return
            if channel is not LiveChannel.REFERENCE_SLOW:
                self._clear_publication_intent(channel)
                return
        if intent.remove_immutable_batch:
            for path in (manifest_path, payload_path):
                with suppress(FileNotFoundError):
                    path.unlink()
        with suppress(FileNotFoundError):
            receipt_path.unlink()
        with suppress(FileNotFoundError):
            signature_path.unlink()
        if intent.previous_pointer is None:
            with suppress(FileNotFoundError):
                current_path.unlink()
        else:
            self._atomic_write(current_path, self._json_bytes(intent.previous_pointer))
        for directory_path in (
            current_path.parent,
            manifest_path.parent,
            receipt_path.parent,
            signature_path.parent,
        ):
            directory_path.mkdir(mode=0o700, parents=True, exist_ok=True)
            directory = os.open(
                directory_path,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        self._clear_publication_intent(channel)

    def _write_publication_intent(self, intent: _SpoolPublicationIntent) -> None:
        path = self._intent_path(intent.channel)
        if path.exists():
            raise LiveSpoolIntegrityError("publication intent already exists")
        self._atomic_write(path, self._json_bytes(intent))

    def publish(
        self,
        envelope: BatchEnvelope,
        payload: bytes,
        *,
        completion_clock: Callable[[], datetime] | None = None,
        not_after: datetime | None = None,
        monotonic_deadline: float | None = None,
    ) -> BatchPointer:
        if self.source_read_only:
            raise LiveSpoolIntegrityError("source read-only spool cannot publish")
        observed_hash = hashlib.sha256(payload).hexdigest()
        if observed_hash != envelope.content_sha256:
            raise LiveSpoolIntegrityError("payload content hash does not match envelope")
        source_generation_id = self._source_generation(envelope.channel)
        if (completion_clock is None) != (not_after is None):
            raise ValueError("completion_clock and not_after must be provided together")
        if envelope.channel is LiveChannel.REFERENCE_SLOW and completion_clock is None:
            raise LiveSpoolIntegrityError(
                "reference slow publications require a completion receipt deadline"
            )
        if monotonic_deadline is not None and completion_clock is None:
            raise ValueError("monotonic_deadline requires completion_clock")
        deadline = normalize_aware_utc(not_after) if not_after is not None else None
        with self._exclusive_lock():
            self._recover_publication_intent_locked(envelope.channel)
            manifest_path = self._manifest_path(envelope.channel, envelope.sequence)
            payload_path = self._payload_path(envelope.channel, envelope.sequence)
            if manifest_path.exists() or payload_path.exists():
                return self._validate_idempotent_replay(
                    envelope=envelope,
                    payload=payload,
                    manifest_path=manifest_path,
                    payload_path=payload_path,
                    completion_clock=completion_clock,
                    not_after=deadline,
                    monotonic_deadline=monotonic_deadline,
                )

            current = self._read_current(envelope.channel)
            expected_sequence = self._next_sequence_locked(envelope.channel)
            if envelope.sequence != expected_sequence:
                raise LiveSpoolIntegrityError(
                    f"next sequence must be {expected_sequence}, got {envelope.sequence}"
                )
            return self._commit_publication_locked(
                envelope=envelope,
                payload=payload,
                source_generation_id=source_generation_id,
                previous=current,
                manifest_path=manifest_path,
                payload_path=payload_path,
                remove_immutable_batch=True,
                advances_current=self._is_current_eligible(envelope.quality_status),
                completion_clock=completion_clock,
                not_after=deadline,
                monotonic_deadline=monotonic_deadline,
            )

    def _commit_publication_locked(
        self,
        *,
        envelope: BatchEnvelope,
        payload: bytes,
        source_generation_id: str,
        previous: CurrentPointer | None,
        manifest_path: Path,
        payload_path: Path,
        remove_immutable_batch: bool,
        advances_current: bool,
        completion_clock: Callable[[], datetime] | None,
        not_after: datetime | None,
        monotonic_deadline: float | None,
    ) -> BatchPointer:
        pointer = BatchPointer(
            channel=envelope.channel,
            source_generation_id=source_generation_id,
            batch_id=envelope.batch_id,
            sequence=envelope.sequence,
            revision=envelope.revision,
            content_sha256=envelope.content_sha256,
            quality_status=envelope.quality_status,
            published_at=envelope.available_at,
        )
        intent = _SpoolPublicationIntent(
            channel=envelope.channel,
            sequence=envelope.sequence,
            source_generation_id=source_generation_id,
            envelope_identity_sha256=envelope.identity_sha256,
            previous_pointer=previous,
            target_pointer=pointer,
            advances_current=advances_current,
            remove_immutable_batch=remove_immutable_batch,
            not_after=not_after,
        )
        self._write_publication_intent(intent)
        if remove_immutable_batch:
            self._atomic_write(payload_path, payload)
            self._atomic_write(manifest_path, self._json_bytes(envelope))

        requires_receipt = advances_current or envelope.channel is LiveChannel.REFERENCE_SLOW
        if requires_receipt and completion_clock is not None and not_after is not None:
            before_replace = normalize_aware_utc(completion_clock())
            visibility_horizon = min(not_after, envelope.available_at)
            if before_replace > visibility_horizon:
                self._recover_publication_intent_locked(envelope.channel)
                raise LiveSpoolIntegrityError("atomic publication missed pre-commit deadline")

        current_path = self._current_path(envelope.channel)
        current_pointer: CurrentPointer | None = None
        if advances_current:
            current_pointer = CurrentPointer.model_validate(pointer.model_dump())
            self._atomic_write(current_path, self._json_bytes(current_pointer))

        if requires_receipt and completion_clock is not None and not_after is not None:
            completed = normalize_aware_utc(completion_clock())
            visibility_horizon = min(not_after, envelope.available_at)
            if completed > visibility_horizon:
                self._recover_publication_intent_locked(envelope.channel)
                raise LiveSpoolIntegrityError("atomic publication completed after deadline")
            if completed < envelope.source_time:
                self._recover_publication_intent_locked(envelope.channel)
                raise LiveSpoolIntegrityError(
                    "atomic publication completion precedes source evidence"
                )
            receipt = _SpoolCompletionReceipt(
                channel=envelope.channel,
                sequence=envelope.sequence,
                source_generation_id=source_generation_id,
                pointer_identity_sha256=pointer.identity_sha256,
                envelope_identity_sha256=envelope.identity_sha256,
                batch_id=envelope.batch_id,
                revision=envelope.revision,
                content_sha256=envelope.content_sha256,
                quality_status=envelope.quality_status,
                producer_commit=envelope.producer_commit,
                source_time=envelope.source_time,
                received_at=envelope.received_at,
                completed_at=completed,
                visible_at=envelope.available_at,
            )
            self._atomic_write(
                self._publication_receipt_path(envelope.channel, envelope.sequence),
                self._json_bytes(receipt),
            )
            receipt_durable_at = normalize_aware_utc(completion_clock())
            if receipt_durable_at > visibility_horizon:
                self._recover_publication_intent_locked(envelope.channel)
                raise LiveSpoolIntegrityError("atomic publication receipt completed after deadline")
            if envelope.channel is LiveChannel.REFERENCE_SLOW and self.source_signer is not None:
                self._write_reference_source_signature(
                    envelope=envelope,
                    payload=payload,
                    receipt=receipt,
                    source_generation_id=source_generation_id,
                )
        try:
            self._clear_publication_intent(envelope.channel)
        except BaseException:
            self._atomic_write(
                self._intent_path(envelope.channel),
                self._json_bytes(intent),
            )
            raise
        if requires_receipt and completion_clock is not None and not_after is not None:
            final_durable_at = normalize_aware_utc(completion_clock())
            visibility_horizon = min(not_after, envelope.available_at)
            if final_durable_at > visibility_horizon or (
                monotonic_deadline is not None and time.monotonic() > monotonic_deadline
            ):
                self._atomic_write(
                    self._intent_path(envelope.channel),
                    self._json_bytes(intent),
                )
                receipt_path = self._publication_receipt_path(
                    envelope.channel,
                    envelope.sequence,
                )
                with suppress(FileNotFoundError):
                    receipt_path.unlink()
                self._fsync_directory(receipt_path.parent)
                self._recover_publication_intent_locked(envelope.channel)
                raise LiveSpoolIntegrityError(
                    "atomic publication finalization completed after deadline"
                )
        return pointer if current_pointer is None else current_pointer

    def _validate_idempotent_replay(
        self,
        *,
        envelope: BatchEnvelope,
        payload: bytes,
        manifest_path: Path,
        payload_path: Path,
        completion_clock: Callable[[], datetime] | None,
        not_after: datetime | None,
        monotonic_deadline: float | None,
    ) -> BatchPointer:
        if not manifest_path.is_file() or not payload_path.is_file():
            raise LiveSpoolIntegrityError("immutable batch is only partially present")
        stored = BatchEnvelope.model_validate_json(manifest_path.read_bytes())
        if stored != envelope or payload_path.read_bytes() != payload:
            raise LiveSpoolIntegrityError("immutable sequence already contains different content")
        pointer = BatchPointer(
            channel=stored.channel,
            source_generation_id=self._source_generation(stored.channel),
            batch_id=stored.batch_id,
            sequence=stored.sequence,
            revision=stored.revision,
            content_sha256=stored.content_sha256,
            quality_status=stored.quality_status,
            published_at=stored.available_at,
        )
        current = self._read_current(stored.channel)
        if self._is_current_eligible(stored.quality_status):
            if current is not None and current.identity_sha256 == pointer.identity_sha256:
                return current
            if current is not None:
                raise LiveSpoolIntegrityError(
                    "current pointer conflicts with immutable batch recovery"
                )
        self._validate_immutable_prefix(
            channel=stored.channel,
            high_watermark=stored.sequence,
        )
        return self._commit_publication_locked(
            envelope=stored,
            payload=payload,
            source_generation_id=pointer.source_generation_id,
            previous=current,
            manifest_path=manifest_path,
            payload_path=payload_path,
            remove_immutable_batch=False,
            advances_current=self._is_current_eligible(stored.quality_status),
            completion_clock=completion_clock,
            not_after=not_after,
            monotonic_deadline=monotonic_deadline,
        )

    def current(self, channel: LiveChannel) -> CurrentPointer | None:
        if self._intent_path(channel).exists():
            if self.source_read_only:
                raise LiveSpoolIntegrityError("pending publication intent requires writer recovery")
            with self._exclusive_lock():
                self._recover_publication_intent_locked(channel)
        return self._read_current(channel)

    def _read_current(self, channel: LiveChannel) -> CurrentPointer | None:
        path = self._current_path(channel)
        if not path.exists():
            return None
        try:
            pointer = CurrentPointer.model_validate_json(
                _secure_read_regular_file(
                    path,
                    label="current pointer",
                    max_bytes=64 * 1024,
                )
            )
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("current pointer is invalid") from exc
        if pointer.channel is not channel:
            raise LiveSpoolIntegrityError("current pointer channel does not match its path")
        if not self._is_current_eligible(pointer.quality_status):
            raise LiveSpoolIntegrityError("current pointer quality is not authoritative")
        if pointer.source_generation_id != self._source_generation(channel):
            raise LiveSpoolIntegrityError("current pointer source generation changed")
        envelopes = self._validate_immutable_prefix(
            channel=channel,
            high_watermark=pointer.sequence,
            allow_trailing=True,
        )
        current_envelope = envelopes[-1]
        if (
            pointer.batch_id != current_envelope.batch_id
            or pointer.revision != current_envelope.revision
            or pointer.content_sha256 != current_envelope.content_sha256
            or pointer.quality_status is not current_envelope.quality_status
            or pointer.published_at != current_envelope.available_at
        ):
            raise LiveSpoolIntegrityError("current pointer does not match immutable manifest")
        return pointer

    @staticmethod
    def _is_current_eligible(quality_status: BatchQualityStatus) -> bool:
        return quality_status is BatchQualityStatus.PUBLISHED

    def _next_sequence_locked(self, channel: LiveChannel) -> int:
        if channel is LiveChannel.REFERENCE_SLOW:
            retired_through = self._reference_retired_through()
            channel_dir = self._channel_dir(channel)
            try:
                manifest_sequences = tuple(
                    int(path.stem) for path in sorted(channel_dir.glob("*.json"))
                )
                payload_sequences = tuple(
                    int(path.stem) for path in sorted(channel_dir.glob("*.payload"))
                )
            except ValueError as exc:
                raise LiveSpoolIntegrityError(
                    "reference immutable prefix contains an invalid sequence filename"
                ) from exc
            if manifest_sequences != payload_sequences:
                raise LiveSpoolIntegrityError(
                    "reference immutable prefix must contain exact manifest/payload pairs"
                )
            if not manifest_sequences:
                return retired_through + 1
            expected = tuple(range(retired_through + 1, manifest_sequences[-1] + 1))
            if manifest_sequences != expected:
                raise LiveSpoolIntegrityError(
                    "reference immutable prefix must contain exact manifest/payload pairs"
                )
            self._validate_immutable_prefix(
                channel=channel,
                high_watermark=manifest_sequences[-1],
            )
            return manifest_sequences[-1] + 1
        channel_dir = self._channel_dir(channel)
        try:
            manifest_sequences = tuple(
                int(path.stem) for path in sorted(channel_dir.glob("*.json"))
            )
            payload_sequences = tuple(
                int(path.stem) for path in sorted(channel_dir.glob("*.payload"))
            )
        except ValueError as exc:
            raise LiveSpoolIntegrityError(
                "immutable prefix contains an invalid sequence filename"
            ) from exc
        if manifest_sequences != payload_sequences:
            raise LiveSpoolIntegrityError(
                "immutable prefix must contain exact manifest/payload pairs"
            )
        if not manifest_sequences:
            return 0
        expected = tuple(range(manifest_sequences[-1] + 1))
        if manifest_sequences != expected:
            raise LiveSpoolIntegrityError(
                "immutable prefix must contain exact manifest/payload pairs"
            )
        self._validate_immutable_prefix(
            channel=channel,
            high_watermark=manifest_sequences[-1],
        )
        return manifest_sequences[-1] + 1

    def _write_reference_source_signature(
        self,
        *,
        envelope: BatchEnvelope,
        payload: bytes,
        receipt: _SpoolCompletionReceipt,
        source_generation_id: str,
    ) -> None:
        signer = self.source_signer
        if signer is None:
            raise LiveSpoolIntegrityError("reference source signing credential is unavailable")
        descriptor_bytes = _secure_read_regular_file(
            self._source_path(envelope.channel),
            label="reference source descriptor",
            max_bytes=64 * 1024,
        )
        manifest_bytes = self._json_bytes(envelope)
        receipt_bytes = self._json_bytes(receipt)
        unsigned = _ReferenceSourceBatchSignature(
            key_id=signer.key_id,
            channel=envelope.channel,
            sequence=envelope.sequence,
            source_generation_id=source_generation_id,
            descriptor_sha256=hashlib.sha256(descriptor_bytes).hexdigest(),
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            payload_sha256=hashlib.sha256(payload).hexdigest(),
            receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
            producer_commit=envelope.producer_commit,
            signature_base64="pending",
        )
        signed = unsigned.model_copy(
            update={"signature_base64": signer.sign(unsigned.signing_payload())}
        )
        self._atomic_write(
            self._source_signature_path(envelope.channel, envelope.sequence),
            self._json_bytes(signed),
        )

    def verify_reference_source_record(self, record: LiveBatchRecord) -> None:
        envelope = record.envelope
        if envelope.channel is not LiveChannel.REFERENCE_SLOW:
            raise LiveSpoolIntegrityError("source signature verification requires reference slow")
        verifier = self.source_verifier
        if verifier is None:
            raise LiveSpoolIntegrityError("reference source verification credential is unavailable")
        source_generation_id = self._source_generation(envelope.channel)
        descriptor_bytes = _secure_read_regular_file(
            self._source_path(envelope.channel),
            label="reference source descriptor",
            max_bytes=64 * 1024,
        )
        manifest_bytes = _secure_read_regular_file(
            record.manifest_path,
            label="reference source manifest",
        )
        payload = _secure_read_regular_file(
            record.payload_path,
            label="reference source payload",
        )
        receipt_path = self._publication_receipt_path(envelope.channel, envelope.sequence)
        receipt_bytes = _secure_read_regular_file(
            receipt_path,
            label="reference source completion receipt",
            max_bytes=64 * 1024,
        )
        signature_path = self._source_signature_path(envelope.channel, envelope.sequence)
        try:
            signature = _ReferenceSourceBatchSignature.model_validate(
                strict_canonical_json_loads(
                    _secure_read_regular_file(
                        signature_path,
                        label="reference source batch signature",
                        max_bytes=128 * 1024,
                    )
                )
            )
        except (ValueError, TypeError) as exc:
            raise LiveSpoolIntegrityError("reference source batch signature is invalid") from exc
        matches = (
            signature.key_id == verifier.key_id
            and signature.channel is envelope.channel
            and signature.sequence == envelope.sequence
            and signature.source_generation_id == source_generation_id
            and signature.descriptor_sha256 == hashlib.sha256(descriptor_bytes).hexdigest()
            and signature.manifest_sha256 == hashlib.sha256(manifest_bytes).hexdigest()
            and signature.payload_sha256 == hashlib.sha256(payload).hexdigest()
            and signature.receipt_sha256 == hashlib.sha256(receipt_bytes).hexdigest()
            and signature.producer_commit == envelope.producer_commit
            and verifier.verify(signature.signing_payload(), signature.signature_base64)
        )
        if not matches:
            raise LiveSpoolIntegrityError("reference source batch signature verification failed")

    @staticmethod
    def _sha256_file(path: Path, *, label: str) -> str:
        return hashlib.sha256(_secure_read_regular_file(path, label=label)).hexdigest()

    def _reference_retirement_source_paths(
        self,
        sequence: int,
    ) -> tuple[Path, Path, Path, Path]:
        return (
            self._manifest_path(LiveChannel.REFERENCE_SLOW, sequence),
            self._payload_path(LiveChannel.REFERENCE_SLOW, sequence),
            self._publication_receipt_path(LiveChannel.REFERENCE_SLOW, sequence),
            self._source_signature_path(LiveChannel.REFERENCE_SLOW, sequence),
        )

    def _validate_reference_retirement_receipt(
        self,
        receipt: ReferenceSpoolRetirementReceipt,
        *,
        receipt_bytes: bytes,
    ) -> None:
        verifier = self.source_verifier
        if verifier is None:
            raise LiveSpoolIntegrityError(
                "reference retirement verification credential is unavailable"
            )
        if (
            receipt.key_id != verifier.key_id
            or receipt.source_generation_id != self._source_generation(LiveChannel.REFERENCE_SLOW)
            or receipt.retired_through_sequence != receipt.batches[-1].sequence
            or tuple(batch.sequence for batch in receipt.batches)
            != tuple(
                range(
                    receipt.previous_retired_through_sequence + 1,
                    receipt.retired_through_sequence + 1,
                )
            )
            or not verifier.verify(receipt.signing_payload(), receipt.signature_base64)
        ):
            raise LiveSpoolIntegrityError("reference retirement receipt is invalid")
        del receipt_bytes
        for batch in receipt.batches:
            archive_paths = self.reference_archive_paths(batch.sequence)
            expected_hashes = (
                batch.manifest_sha256,
                batch.payload_sha256,
                batch.publication_receipt_sha256,
                batch.source_signature_sha256,
            )
            for path, expected in zip(archive_paths, expected_hashes, strict=True):
                if self._sha256_file(path, label="reference cold archive") != expected:
                    raise LiveSpoolIntegrityError("reference cold archive hash mismatch")

    def _reference_retired_through(self) -> int:
        if self.reference_retirement_intent_path.exists():
            raise LiveSpoolIntegrityError("pending reference retirement requires writer recovery")
        path = self.reference_retirement_index_path
        if not path.exists():
            return -1
        try:
            index = _ReferenceSpoolRetentionIndex.model_validate_json(
                _secure_read_regular_file(
                    path,
                    label="reference retirement index",
                    max_bytes=64 * 1024,
                )
            )
            receipt_path = self.reference_retirement_receipt_path(index.retired_through_sequence)
            receipt_bytes = _secure_read_regular_file(
                receipt_path,
                label="reference retirement receipt",
                max_bytes=512 * 1024,
            )
            if hashlib.sha256(receipt_bytes).hexdigest() != index.receipt_sha256:
                raise LiveSpoolIntegrityError("reference retirement receipt hash mismatch")
            receipt = ReferenceSpoolRetirementReceipt.model_validate_json(receipt_bytes)
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("reference retirement state is invalid") from exc
        if (
            index.source_generation_id != self._source_generation(LiveChannel.REFERENCE_SLOW)
            or receipt.retired_through_sequence != index.retired_through_sequence
        ):
            raise LiveSpoolIntegrityError("reference retirement state identity changed")
        self._validate_reference_retirement_receipt(
            receipt,
            receipt_bytes=receipt_bytes,
        )
        return index.retired_through_sequence

    def _load_reference_retirement_intent(self) -> _ReferenceSpoolRetirementIntent | None:
        if not self.reference_retirement_intent_path.exists():
            return None
        try:
            return _ReferenceSpoolRetirementIntent.model_validate_json(
                _secure_read_regular_file(
                    self.reference_retirement_intent_path,
                    label="reference retirement intent",
                    max_bytes=512 * 1024,
                )
            )
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("reference retirement intent is invalid") from exc

    def _reference_retirement_digest(self, sequence: int) -> ReferenceRetiredBatchDigest:
        paths = self._reference_retirement_source_paths(sequence)
        return ReferenceRetiredBatchDigest(
            sequence=sequence,
            manifest_sha256=self._sha256_file(paths[0], label="reference manifest"),
            payload_sha256=self._sha256_file(paths[1], label="reference payload"),
            publication_receipt_sha256=self._sha256_file(
                paths[2],
                label="reference publication receipt",
            ),
            source_signature_sha256=self._sha256_file(
                paths[3],
                label="reference source signature",
            ),
        )

    def _complete_reference_retirement(
        self,
        intent: _ReferenceSpoolRetirementIntent,
        *,
        fault_injector: Callable[[str], None] | None,
    ) -> ReferenceSpoolRetirementReceipt:
        receipt = intent.receipt
        first_move = True
        for batch in receipt.batches:
            source_paths = self._reference_retirement_source_paths(batch.sequence)
            archive_paths = self.reference_archive_paths(batch.sequence)
            expected_hashes = (
                batch.manifest_sha256,
                batch.payload_sha256,
                batch.publication_receipt_sha256,
                batch.source_signature_sha256,
            )
            for source, archive, expected_hash in zip(
                source_paths,
                archive_paths,
                expected_hashes,
                strict=True,
            ):
                source_exists = source.exists()
                archive_exists = archive.exists()
                if source_exists and archive_exists:
                    raise LiveSpoolIntegrityError(
                        "reference retirement has duplicate hot/cold files"
                    )
                if not source_exists and not archive_exists:
                    raise LiveSpoolIntegrityError("reference retirement lost an immutable file")
                if source_exists:
                    if (
                        self._sha256_file(source, label="reference retirement source")
                        != expected_hash
                    ):
                        raise LiveSpoolIntegrityError("reference retirement source hash mismatch")
                    archive.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    os.replace(source, archive)
                    self._fsync_directory(source.parent)
                    self._fsync_directory(archive.parent)
                    if first_move and fault_injector is not None:
                        first_move = False
                        fault_injector("after_first_archive_move")
                if self._sha256_file(archive, label="reference cold archive") != expected_hash:
                    raise LiveSpoolIntegrityError("reference cold archive hash mismatch")

        receipt_path = self.reference_retirement_receipt_path(receipt.retired_through_sequence)
        receipt_bytes = self._json_bytes(receipt)
        if receipt_path.exists():
            if self._sha256_file(receipt_path, label="reference retirement receipt") != (
                hashlib.sha256(receipt_bytes).hexdigest()
            ):
                raise LiveSpoolIntegrityError("reference retirement receipt conflicts")
        else:
            self._atomic_write(receipt_path, receipt_bytes)
        self._atomic_write(
            self.reference_retirement_index_path,
            self._json_bytes(
                _ReferenceSpoolRetentionIndex(
                    source_generation_id=receipt.source_generation_id,
                    retired_through_sequence=receipt.retired_through_sequence,
                    receipt_sha256=hashlib.sha256(receipt_bytes).hexdigest(),
                )
            ),
        )
        with suppress(FileNotFoundError):
            self.reference_retirement_intent_path.unlink()
        self._fsync_directory(self.reference_retirement_root)
        return receipt

    def retire_reference_batches_single_consumer(
        self,
        *,
        cursor: ConsumerCursor,
        retain_hot_batches: int,
        max_batches: int,
        retired_at: datetime,
        fault_injector: Callable[[str], None] | None = None,
    ) -> ReferenceSpoolRetirementReceipt | None:
        """Archive one bounded page only after the configured consumer committed it."""

        if self.source_read_only:
            raise LiveSpoolIntegrityError("source read-only spool cannot retire reference batches")
        if cursor.channel is not LiveChannel.REFERENCE_SLOW:
            raise ValueError("reference retention cursor channel is invalid")
        if retain_hot_batches < 0 or max_batches < 1 or max_batches > 256:
            raise ValueError("reference retention budgets are invalid")
        with self._exclusive_lock():
            pending = self._load_reference_retirement_intent()
            if pending is not None:
                if (
                    pending.receipt.consumer_id != cursor.consumer_id
                    or pending.receipt.source_generation_id != cursor.source_generation_id
                ):
                    raise LiveSpoolIntegrityError("reference retirement cursor identity changed")
                return self._complete_reference_retirement(
                    pending,
                    fault_injector=fault_injector,
                )

            current = self._read_current(LiveChannel.REFERENCE_SLOW)
            if current is None:
                return None
            if cursor.source_generation_id != current.source_generation_id:
                raise LiveSpoolIntegrityError("reference retention cursor generation changed")
            retired_through = self._reference_retired_through()
            target = min(
                cursor.last_sequence,
                current.sequence - max(retain_hot_batches, 1),
                retired_through + max_batches,
            )
            if target <= retired_through:
                return None
            batches = tuple(
                self._reference_retirement_digest(sequence)
                for sequence in range(retired_through + 1, target + 1)
            )
            signer = self.source_signer
            if signer is None:
                raise LiveSpoolIntegrityError(
                    "reference retirement requires the source signing credential"
                )
            previous_receipt_sha256 = "0" * 64
            if retired_through >= 0:
                previous_receipt_sha256 = self._sha256_file(
                    self.reference_retirement_receipt_path(retired_through),
                    label="previous reference retirement receipt",
                )
            unsigned = ReferenceSpoolRetirementReceipt(
                source_generation_id=current.source_generation_id,
                consumer_id=cursor.consumer_id,
                previous_retired_through_sequence=retired_through,
                retired_through_sequence=target,
                previous_receipt_sha256=previous_receipt_sha256,
                batches=batches,
                retired_at=normalize_aware_utc(retired_at),
                key_id=signer.key_id,
                signature_base64="pending",
            )
            receipt = unsigned.model_copy(
                update={"signature_base64": signer.sign(unsigned.signing_payload())}
            )
            intent = _ReferenceSpoolRetirementIntent(receipt=receipt)
            self._atomic_write(
                self.reference_retirement_intent_path,
                self._json_bytes(intent),
            )
            return self._complete_reference_retirement(
                intent,
                fault_injector=fault_injector,
            )

    def _validate_publication_receipt(
        self,
        envelope: BatchEnvelope,
        *,
        source_generation_id: str,
    ) -> None:
        path = self._publication_receipt_path(envelope.channel, envelope.sequence)
        if not path.exists():
            if envelope.channel is LiveChannel.REFERENCE_SLOW:
                raise LiveSpoolIntegrityError("reference slow current lacks a completion receipt")
            return
        try:
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.getuid()
                or stat.S_IMODE(observed.st_mode) != 0o600
            ):
                raise LiveSpoolIntegrityError("spool completion receipt is unsafe")
            receipt = _SpoolCompletionReceipt.model_validate(
                strict_canonical_json_loads(
                    _secure_read_regular_file(
                        path,
                        label="spool completion receipt",
                        max_bytes=64 * 1024,
                    )
                )
            )
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("spool completion receipt is invalid") from exc
        pointer = BatchPointer(
            channel=envelope.channel,
            source_generation_id=source_generation_id,
            batch_id=envelope.batch_id,
            sequence=envelope.sequence,
            revision=envelope.revision,
            content_sha256=envelope.content_sha256,
            quality_status=envelope.quality_status,
            published_at=envelope.available_at,
        )
        if not (
            envelope.event_time_end
            <= envelope.source_time
            <= envelope.received_at
            <= envelope.available_at
        ):
            raise LiveSpoolIntegrityError("spool source evidence time relationship is invalid")
        if (
            receipt.channel is not envelope.channel
            or receipt.sequence != envelope.sequence
            or receipt.source_generation_id != source_generation_id
            or receipt.pointer_identity_sha256 != pointer.identity_sha256
            or receipt.envelope_identity_sha256 != envelope.identity_sha256
            or receipt.batch_id != envelope.batch_id
            or receipt.revision != envelope.revision
            or receipt.content_sha256 != envelope.content_sha256
            or receipt.quality_status is not envelope.quality_status
            or receipt.producer_commit != envelope.producer_commit
            or receipt.source_time != envelope.source_time
            or receipt.received_at != envelope.received_at
            or receipt.visible_at != envelope.available_at
            or receipt.completed_at < envelope.source_time
            or receipt.completed_at > receipt.visible_at
        ):
            raise LiveSpoolIntegrityError(
                "spool completion receipt does not match immutable manifest"
            )

    def _validate_immutable_prefix(
        self,
        *,
        channel: LiveChannel,
        high_watermark: int,
        allow_trailing: bool = False,
    ) -> tuple[BatchEnvelope, ...]:
        channel_dir = self._channel_dir(channel)
        if channel is LiveChannel.REFERENCE_SLOW:
            retired_through = self._reference_retired_through()
            window_start = max(retired_through + 1, high_watermark - 127)
            expected_sequences = tuple(range(window_start, high_watermark + 1))
        else:
            try:
                manifest_sequences = tuple(
                    int(path.stem) for path in sorted(channel_dir.glob("*.json"))
                )
                payload_sequences = tuple(
                    int(path.stem) for path in sorted(channel_dir.glob("*.payload"))
                )
            except ValueError as exc:
                raise LiveSpoolIntegrityError(
                    "immutable prefix contains an invalid sequence filename"
                ) from exc
            expected_sequences = tuple(range(high_watermark + 1))
            if allow_trailing:
                manifests_match = (
                    manifest_sequences[: len(expected_sequences)] == expected_sequences
                )
                payloads_match = payload_sequences[: len(expected_sequences)] == expected_sequences
            else:
                manifests_match = manifest_sequences == expected_sequences
                payloads_match = payload_sequences == expected_sequences
            if not manifests_match or not payloads_match:
                raise LiveSpoolIntegrityError(
                    "immutable prefix must contain exact manifest/payload pairs"
                )

        envelopes: list[BatchEnvelope] = []
        source_generation_id = self._source_generation(channel)
        for sequence in expected_sequences:
            manifest_path = self._manifest_path(channel, sequence)
            payload_path = self._payload_path(channel, sequence)
            try:
                envelope = BatchEnvelope.model_validate_json(
                    _secure_read_regular_file(
                        manifest_path,
                        label="immutable batch manifest",
                    )
                )
                payload = _secure_read_regular_file(
                    payload_path,
                    label="immutable batch payload",
                )
            except (OSError, ValueError) as exc:
                raise LiveSpoolIntegrityError("immutable prefix is invalid") from exc
            if envelope.channel is not channel or envelope.sequence != sequence:
                raise LiveSpoolIntegrityError("immutable prefix identity changed")
            if hashlib.sha256(payload).hexdigest() != envelope.content_sha256:
                raise LiveSpoolIntegrityError("immutable prefix payload is corrupt")
            if (
                channel is LiveChannel.REFERENCE_SLOW
                or self._publication_receipt_path(
                    channel,
                    sequence,
                ).exists()
            ):
                self._validate_publication_receipt(
                    envelope,
                    source_generation_id=source_generation_id,
                )
            envelopes.append(envelope)
        return tuple(envelopes)

    def source_descriptor(self, channel: LiveChannel) -> LiveSourceDescriptor:
        current = self.current(channel)
        return LiveSourceDescriptor(
            channel=channel,
            generation_id=self._source_generation(channel),
            high_watermark=-1 if current is None else current.sequence,
        )

    def list_after(
        self,
        channel: LiveChannel,
        *,
        sequence: int,
        limit: int | None = None,
    ) -> tuple[LiveBatchRecord, ...]:
        if channel is LiveChannel.REFERENCE_SLOW:
            page_limit = 64 if limit is None else limit
            if page_limit < 1 or page_limit > 256:
                raise ValueError("reference spool page limit must be between 1 and 256")
            current = self.current(channel)
            if current is None or sequence >= current.sequence:
                return ()
            retired_through = self._reference_retired_through()
            if sequence < retired_through:
                raise LiveSpoolIntegrityError(
                    "reference consumer cursor is behind retired cold archive"
                )
            start = max(sequence + 1, 0)
            stop = min(current.sequence + 1, start + page_limit)
            records: list[LiveBatchRecord] = []
            for expected_sequence in range(start, stop):
                path = self._manifest_path(channel, expected_sequence)
                try:
                    envelope = BatchEnvelope.model_validate_json(
                        _secure_read_regular_file(path, label="batch manifest")
                    )
                except (OSError, ValueError) as exc:
                    raise LiveSpoolIntegrityError(f"invalid batch manifest: {path.name}") from exc
                if envelope.channel is not channel or envelope.sequence != expected_sequence:
                    raise LiveSpoolIntegrityError(
                        "batch manifest channel or sequence does not match its path"
                    )
                payload_path = self._payload_path(channel, expected_sequence)
                payload = _secure_read_regular_file(payload_path, label="batch payload")
                if hashlib.sha256(payload).hexdigest() != envelope.content_sha256:
                    raise LiveSpoolIntegrityError("batch payload content hash mismatch")
                self._validate_publication_receipt(
                    envelope,
                    source_generation_id=current.source_generation_id,
                )
                records.append(
                    LiveBatchRecord(
                        envelope=envelope,
                        manifest_path=path,
                        payload_path=payload_path,
                    )
                )
            return tuple(records)

        records: list[LiveBatchRecord] = []
        self._next_sequence_locked(channel)
        for path in sorted(self._channel_dir(channel).glob("*.json")):
            try:
                envelope = BatchEnvelope.model_validate_json(
                    _secure_read_regular_file(path, label="batch manifest")
                )
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
        if not records:
            return ()
        expected = list(range(max(sequence + 1, 0), records[-1].envelope.sequence + 1))
        observed = [record.envelope.sequence for record in records]
        if observed != expected:
            raise LiveSpoolIntegrityError(
                f"batch sequence gap: expected {expected}, observed {observed}"
            )
        return tuple(records)

    def read_payload(self, record: LiveBatchRecord) -> bytes:
        try:
            payload = _secure_read_regular_file(
                record.payload_path,
                label="batch payload",
            )
        except OSError as exc:
            raise LiveSpoolIntegrityError("batch payload is unavailable") from exc
        if hashlib.sha256(payload).hexdigest() != record.envelope.content_sha256:
            raise LiveSpoolIntegrityError("batch payload content hash mismatch")
        return payload

    def commit_cursor(self, cursor: ConsumerCursor) -> None:
        self._require_cursor_writer()
        with self._exclusive_lock(self._cursor_lock_path):
            self._recover_cursor_intent_locked(cursor.consumer_id, cursor.channel)
            existing = self._read_cursor(cursor.consumer_id, cursor.channel)
            self._commit_cursor_publication_locked(
                cursor,
                existing=existing,
                completion_clock=None,
                not_after=None,
                retain_intent=False,
            )

    def commit_cursor_with_deadline(
        self,
        cursor: ConsumerCursor,
        *,
        completion_clock: Callable[[], datetime],
        not_after: datetime,
        retain_intent: bool = False,
        publication_id: str | None = None,
        completion_receipt_path: Path | None = None,
        registry_generation_id: str | None = None,
    ) -> None:
        self._require_cursor_writer()
        if (publication_id is None) != (completion_receipt_path is None):
            raise ValueError("publication_id and completion_receipt_path must be provided together")
        if publication_id is not None and registry_generation_id is None:
            raise ValueError("registry_generation_id is required for shared publication")
        deadline = normalize_aware_utc(not_after)
        with self._exclusive_lock(self._cursor_lock_path):
            self._recover_cursor_intent_locked(cursor.consumer_id, cursor.channel)
            existing = self._read_cursor(cursor.consumer_id, cursor.channel)
            self._commit_cursor_publication_locked(
                cursor,
                existing=existing,
                completion_clock=completion_clock,
                not_after=deadline,
                retain_intent=retain_intent,
                publication_id=publication_id,
                completion_receipt_path=completion_receipt_path,
                registry_generation_id=registry_generation_id,
            )

    def _commit_cursor_publication_locked(
        self,
        cursor: ConsumerCursor,
        *,
        existing: ConsumerCursor | None,
        completion_clock: Callable[[], datetime] | None,
        not_after: datetime | None,
        retain_intent: bool,
        publication_id: str | None = None,
        completion_receipt_path: Path | None = None,
        registry_generation_id: str | None = None,
    ) -> None:
        self._validate_cursor_transition(cursor, existing=existing)
        intent = _CursorPublicationIntent(
            consumer_id=cursor.consumer_id,
            channel=cursor.channel,
            source_generation_id=cursor.source_generation_id,
            previous_cursor=existing,
            target_cursor=cursor,
            not_after=not_after,
            publication_id=publication_id,
            completion_receipt_path=(
                str(completion_receipt_path) if completion_receipt_path else None
            ),
            registry_generation_id=registry_generation_id,
        )
        self._atomic_write(
            self._cursor_intent_path(cursor.consumer_id, cursor.channel),
            self._json_bytes(intent),
        )
        if completion_clock is not None and not_after is not None:
            before_replace = normalize_aware_utc(completion_clock())
            if before_replace > not_after:
                self._recover_cursor_intent_locked(cursor.consumer_id, cursor.channel)
                raise LiveSpoolIntegrityError("cursor publication missed pre-commit deadline")
        self._atomic_write(
            self._cursor_path(cursor.consumer_id, cursor.channel),
            self._json_bytes(cursor),
        )
        if completion_clock is not None and not_after is not None:
            completed = normalize_aware_utc(completion_clock())
            if completed > not_after:
                self._recover_cursor_intent_locked(cursor.consumer_id, cursor.channel)
                raise LiveSpoolIntegrityError("cursor publication completed after deadline")
        if not retain_intent:
            self._clear_cursor_intent(cursor.consumer_id, cursor.channel)

    def _validate_cursor_transition(
        self,
        cursor: ConsumerCursor,
        *,
        existing: ConsumerCursor | None,
    ) -> None:
        if cursor.source_generation_id != self._source_generation(cursor.channel):
            raise LiveSpoolIntegrityError("consumer source generation changed")
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

    def _clear_cursor_intent(self, consumer_id: str, channel: LiveChannel) -> None:
        path = self._cursor_intent_path(consumer_id, channel)
        with suppress(FileNotFoundError):
            path.unlink()
        directory = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def _cursor_completion_receipt(
        self,
        intent: _CursorPublicationIntent,
    ) -> ReferencePublicationCompletionReceipt | None:
        if (
            intent.publication_id is None
            or intent.completion_receipt_path is None
            or intent.registry_generation_id is None
        ):
            return None
        expected_path = self.completion_receipt_path(intent.publication_id)
        path = Path(intent.completion_receipt_path)
        if path != expected_path:
            raise LiveSpoolIntegrityError("cursor completion receipt path changed")
        uncommitted_path = self.completion_receipt_intent_path(intent.publication_id)
        if uncommitted_path.exists():
            return None
        if not path.exists():
            return None
        try:
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.getuid()
                or stat.S_IMODE(observed.st_mode) != 0o600
            ):
                raise LiveSpoolIntegrityError("cursor completion receipt is unsafe")
            receipt = ReferencePublicationCompletionReceipt.model_validate(
                strict_canonical_json_loads(path.read_bytes())
            )
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("cursor completion receipt is invalid") from exc
        if (
            receipt.publication_id != intent.publication_id
            or receipt.registry_generation_id != intent.registry_generation_id
            or receipt.target_cursor != intent.target_cursor
            or receipt.source_generation_id != intent.source_generation_id
            or receipt.channel is not intent.channel
            or receipt.deadline != intent.not_after
        ):
            raise LiveSpoolIntegrityError("cursor completion receipt does not match intent")
        expected_intent = ReferencePublicationCommitIntent(
            publication_id=receipt.publication_id,
            registry_generation_id=receipt.registry_generation_id,
            target_cursor=receipt.target_cursor,
            source_generation_id=receipt.source_generation_id,
            channel=receipt.channel,
            deadline=receipt.deadline,
            stage_sha256=receipt.stage_sha256,
            key_id=receipt.key_id,
        )
        if receipt.intent_sha256 != expected_intent.content_sha256:
            raise LiveSpoolIntegrityError("cursor completion receipt intent hash changed")
        authenticator = self.publication_authenticator
        if (
            authenticator is None
            or receipt.key_id != authenticator.key_id
            or not authenticator.verify(
                receipt.authentication_payload(),
                receipt.authentication_mac,
            )
        ):
            raise LiveSpoolIntegrityError("cursor completion receipt authentication failed")
        if self._validated_completion_evidence(receipt) is None:
            return None
        return receipt

    def _recover_cursor_intent_locked(
        self,
        consumer_id: str,
        channel: LiveChannel,
    ) -> None:
        self._require_cursor_writer()
        intent_path = self._cursor_intent_path(consumer_id, channel)
        if not intent_path.exists():
            return
        try:
            intent = _CursorPublicationIntent.model_validate_json(intent_path.read_bytes())
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("cursor publication intent is invalid") from exc
        if intent.consumer_id != consumer_id or intent.channel is not channel:
            raise LiveSpoolIntegrityError("cursor publication intent identity mismatch")
        if intent.source_generation_id != self._source_generation(channel):
            raise LiveSpoolIntegrityError("cursor publication intent generation changed")
        observed = self._read_cursor(consumer_id, channel)
        if observed not in {intent.previous_cursor, intent.target_cursor}:
            raise LiveSpoolIntegrityError("cursor publication intent found an unrecoverable cursor")
        if self._cursor_completion_receipt(intent) is not None:
            if observed != intent.target_cursor:
                raise LiveSpoolIntegrityError("completed cursor publication does not match target")
            self._clear_cursor_intent(consumer_id, channel)
            return
        path = self._cursor_path(consumer_id, channel)
        if intent.previous_cursor is None:
            with suppress(FileNotFoundError):
                path.unlink()
            directory = os.open(
                path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        else:
            self._atomic_write(path, self._json_bytes(intent.previous_cursor))
        self._clear_cursor_intent(consumer_id, channel)

    def complete_cursor_publication(
        self,
        consumer_id: str,
        channel: LiveChannel,
    ) -> None:
        self._require_cursor_writer()
        with self._exclusive_lock(self._cursor_lock_path):
            intent_path = self._cursor_intent_path(consumer_id, channel)
            if not intent_path.exists():
                return
            try:
                intent = _CursorPublicationIntent.model_validate_json(intent_path.read_bytes())
            except (OSError, ValueError) as exc:
                raise LiveSpoolIntegrityError("cursor publication intent is invalid") from exc
            observed = self._read_cursor(consumer_id, channel)
            if observed != intent.target_cursor:
                raise LiveSpoolIntegrityError("cursor completion does not match pending target")
            if (
                intent.publication_id is not None
                and self._cursor_completion_receipt(intent) is None
            ):
                raise LiveSpoolIntegrityError("cursor completion receipt is missing")
            self._clear_cursor_intent(consumer_id, channel)

    def abort_cursor_publication(
        self,
        consumer_id: str,
        channel: LiveChannel,
    ) -> None:
        """Force a pending shared cursor publication back to its previous cursor."""

        self._require_cursor_writer()
        with self._exclusive_lock(self._cursor_lock_path):
            intent_path = self._cursor_intent_path(consumer_id, channel)
            if not intent_path.exists():
                return
            try:
                intent = _CursorPublicationIntent.model_validate_json(intent_path.read_bytes())
            except (OSError, ValueError) as exc:
                raise LiveSpoolIntegrityError("cursor publication intent is invalid") from exc
            observed = self._read_cursor(consumer_id, channel)
            if observed not in {intent.previous_cursor, intent.target_cursor}:
                raise LiveSpoolIntegrityError(
                    "cursor publication intent found an unrecoverable cursor"
                )
            cursor_path = self._cursor_path(consumer_id, channel)
            if intent.previous_cursor is None:
                with suppress(FileNotFoundError):
                    cursor_path.unlink()
                self._fsync_directory(cursor_path.parent)
            else:
                self._atomic_write(cursor_path, self._json_bytes(intent.previous_cursor))
            self._clear_cursor_intent(consumer_id, channel)

    def write_completion_receipt(
        self,
        *,
        publication_id: str,
        registry_generation_id: str,
        cursor: ConsumerCursor,
        stage_sha256: str,
        completion_clock: Callable[[], datetime],
        not_after: datetime,
        monotonic_deadline: float | None = None,
    ) -> ReferencePublicationCompletionReceipt:
        """Commit a completion receipt, or fail closed when the deadline is crossed.

        Durable-evidence contract (normative; the code below is the only place that
        implements it):

        * Only the two checkpoints that run *after* the receipt itself is durable and
          the commit intent has been cleared emit ``rolled_back_deadline`` evidence:
          the ``final_durable_at`` check and the ``evidence_durable_at`` check. Both
          rewrite the commit intent, unlink the receipt, fsync the directory and only
          then seal the evidence.
        * Every earlier checkpoint rolls the publication back and deliberately emits
          **no** evidence at all. "No evidence" is the contract at each of them, not
          an omission, but for two different reasons:

          - The registry publication stage, the cursor publication stage and
            ``durable_completed_at`` (commit intent written, receipt not yet written)
            all run before the receipt object exists.
            ``ReferencePublicationDurableEvidence.create_authenticated`` is bound to a
            receipt - it signs that receipt's content hash, stage hash and deadline -
            so at those points there is nothing to sign.
          - ``marker_durable_at`` runs after the receipt has been written and fsynced,
            so a receipt does exist there. It stays evidence-free because its rollback
            branch unlinks that receipt again while the commit intent is still on disk,
            never cleared: nothing ever acknowledged the receipt, so evidence would
            attest to a publication that was never visible to anyone.
        * Missing evidence is therefore not an audit gap in the recovery sense:
          :meth:`_validated_completion_evidence` returns ``None`` both when the
          evidence file is absent and when its ``outcome`` is not ``"committed"``, so
          recovery adjudicates "no evidence" and "``rolled_back_deadline`` evidence"
          identically - the receipt is refused and the publication is rolled back.

        ``tests/unit/test_reference_slow_runtime.py`` pins each branch of this
        contract: the two evidence-emitting checkpoints have one deterministic case
        each, and ``test_registry_stage_crossing_cutoff_rolls_back_without_durable_evidence``
        pins the no-evidence branch.
        """

        self._require_cursor_writer()
        deadline = normalize_aware_utc(not_after)
        authenticator = self.publication_authenticator
        if authenticator is None:
            raise LiveSpoolIntegrityError("reference publication authenticator is unavailable")
        intent = ReferencePublicationCommitIntent(
            publication_id=publication_id,
            registry_generation_id=registry_generation_id,
            target_cursor=cursor,
            source_generation_id=cursor.source_generation_id,
            channel=cursor.channel,
            deadline=deadline,
            stage_sha256=stage_sha256,
            key_id=authenticator.key_id,
        )
        path = self.completion_receipt_path(publication_id)
        intent_path = self.completion_receipt_intent_path(publication_id)
        self._atomic_write(intent_path, intent.canonical_json_bytes())
        durable_completed_at = normalize_aware_utc(completion_clock())
        if durable_completed_at > deadline:
            raise LiveSpoolIntegrityError("completion receipt missed pre-commit deadline")
        receipt = ReferencePublicationCompletionReceipt.create_authenticated(
            publication_id=publication_id,
            registry_generation_id=registry_generation_id,
            target_cursor=cursor,
            source_generation_id=cursor.source_generation_id,
            channel=cursor.channel,
            deadline=deadline,
            durable_completed_at=durable_completed_at,
            intent_sha256=intent.content_sha256,
            stage_sha256=stage_sha256,
            authenticator=authenticator,
        )
        self._atomic_write(path, receipt.canonical_json_bytes())
        marker_durable_at = normalize_aware_utc(completion_clock())
        if marker_durable_at > deadline:
            with suppress(FileNotFoundError):
                path.unlink()
            directory = os.open(
                path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            raise LiveSpoolIntegrityError("completion receipt completed after deadline")
        try:
            self._clear_completion_receipt_intent(publication_id)
        except BaseException:
            self._atomic_write(intent_path, intent.canonical_json_bytes())
            raise
        final_durable_at = normalize_aware_utc(completion_clock())
        if final_durable_at > deadline or (
            monotonic_deadline is not None and time.monotonic() > monotonic_deadline
        ):
            self._atomic_write(intent_path, intent.canonical_json_bytes())
            with suppress(FileNotFoundError):
                path.unlink()
            self._fsync_directory(path.parent)
            self._persist_completion_evidence(
                receipt,
                durable_completed_at=final_durable_at,
                outcome="rolled_back_deadline",
            )
            raise LiveSpoolIntegrityError("completion receipt completed after deadline")
        self._persist_completion_evidence(
            receipt,
            durable_completed_at=final_durable_at,
            outcome="committed",
        )
        evidence_durable_at = normalize_aware_utc(completion_clock())
        if evidence_durable_at > deadline or (
            monotonic_deadline is not None and time.monotonic() > monotonic_deadline
        ):
            self._atomic_write(intent_path, intent.canonical_json_bytes())
            with suppress(FileNotFoundError):
                path.unlink()
            self._fsync_directory(path.parent)
            self._persist_completion_evidence(
                receipt,
                durable_completed_at=evidence_durable_at,
                outcome="rolled_back_deadline",
            )
            raise LiveSpoolIntegrityError("completion evidence completed after deadline")
        return receipt

    def _validated_completion_evidence(
        self,
        receipt: ReferencePublicationCompletionReceipt,
    ) -> ReferencePublicationDurableEvidence | None:
        path = self.completion_evidence_root / f"{receipt.publication_id}.json"
        if not path.exists():
            return None
        try:
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.getuid()
                or observed.st_nlink != 1
                or stat.S_IMODE(observed.st_mode) != 0o600
            ):
                raise LiveSpoolIntegrityError("reference publication completion evidence is unsafe")
            evidence = ReferencePublicationDurableEvidence.model_validate(
                strict_canonical_json_loads(path.read_bytes())
            )
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError(
                "reference publication completion evidence is invalid"
            ) from exc
        authenticator = self.publication_authenticator
        if (
            evidence.publication_id != receipt.publication_id
            or evidence.stage_sha256 != receipt.stage_sha256
            or evidence.receipt_content_sha256 != receipt.content_sha256
            or evidence.deadline != receipt.deadline
            or evidence.outcome != "committed"
            or evidence.durable_completed_at > receipt.deadline
            or authenticator is None
            or evidence.key_id != authenticator.key_id
            or not authenticator.verify(
                evidence.authentication_payload(),
                evidence.authentication_mac,
            )
        ):
            return None
        return evidence

    def _persist_completion_evidence(
        self,
        receipt: ReferencePublicationCompletionReceipt,
        *,
        durable_completed_at: datetime,
        outcome: str,
    ) -> None:
        authenticator = self.publication_authenticator
        if authenticator is None:
            raise LiveSpoolIntegrityError("reference publication authenticator is unavailable")
        evidence = ReferencePublicationDurableEvidence.create_authenticated(
            receipt=receipt,
            durable_completed_at=durable_completed_at,
            outcome=outcome,
            authenticator=authenticator,
        )
        self._atomic_write(
            self.completion_evidence_root / f"{receipt.publication_id}.json",
            canonical_json_bytes(evidence.model_dump(mode="json")),
        )

    def finalize_deadline_rollback_evidence(
        self,
        publication_id: str,
        *,
        durable_completed_at: datetime,
    ) -> None:
        """Seal evidence only after registry and cursor rollback are durable."""

        self._require_cursor_writer()
        path = self.completion_evidence_root / f"{publication_id}.json"
        if not path.exists():
            return
        authenticator = self.publication_authenticator
        if authenticator is None:
            raise LiveSpoolIntegrityError("reference publication authenticator is unavailable")
        try:
            evidence = ReferencePublicationDurableEvidence.model_validate(
                strict_canonical_json_loads(path.read_bytes())
            )
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError(
                "reference publication completion evidence is invalid"
            ) from exc
        if (
            evidence.publication_id != publication_id
            or evidence.outcome != "rolled_back_deadline"
            or evidence.key_id != authenticator.key_id
            or not authenticator.verify(
                evidence.authentication_payload(),
                evidence.authentication_mac,
            )
        ):
            raise LiveSpoolIntegrityError(
                "reference publication completion evidence authentication failed"
            )
        unsigned = evidence.model_copy(
            update={
                "durable_completed_at": normalize_aware_utc(durable_completed_at),
                "authentication_mac": "0" * 64,
            }
        )
        sealed = unsigned.model_copy(
            update={"authentication_mac": authenticator.sign(unsigned.authentication_payload())}
        )
        self._atomic_write(path, canonical_json_bytes(sealed.model_dump(mode="json")))

    def _clear_completion_receipt_intent(self, publication_id: str) -> None:
        path = self.completion_receipt_intent_path(publication_id)
        with suppress(FileNotFoundError):
            path.unlink()
        directory = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def remove_completion_receipt(self, publication_id: str) -> None:
        self._require_cursor_writer()
        path = self.completion_receipt_path(publication_id)
        with suppress(FileNotFoundError):
            path.unlink()
        intent_path = self.completion_receipt_intent_path(publication_id)
        with suppress(FileNotFoundError):
            intent_path.unlink()
        directory = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def load_cursor(
        self,
        consumer_id: str,
        channel: LiveChannel,
    ) -> ConsumerCursor | None:
        if self._cursor_intent_path(consumer_id, channel).exists():
            if self.read_only:
                raise LiveSpoolIntegrityError("pending cursor publication requires writer recovery")
            with self._exclusive_lock(self._cursor_lock_path):
                self._recover_cursor_intent_locked(consumer_id, channel)
        return self._read_cursor(consumer_id, channel)

    def _read_cursor(
        self,
        consumer_id: str,
        channel: LiveChannel,
    ) -> ConsumerCursor | None:
        path = self._cursor_path(consumer_id, channel)
        if not path.exists():
            return None
        try:
            cursor = ConsumerCursor.model_validate_json(
                _secure_read_regular_file(
                    path,
                    label="consumer cursor",
                    max_bytes=64 * 1024,
                )
            )
        except (OSError, ValueError) as exc:
            raise LiveSpoolIntegrityError("consumer cursor is invalid") from exc
        if cursor.consumer_id != consumer_id or cursor.channel is not channel:
            raise LiveSpoolIntegrityError("consumer cursor identity mismatch")
        if cursor.source_generation_id != self._source_generation(channel):
            raise LiveSpoolIntegrityError("consumer source generation changed")
        return cursor
