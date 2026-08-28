"""Append-only point-in-time registry for slowly changing reference data."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import hmac
import json
import os
import sqlite3
import stat
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from enum import StrEnum
from itertools import pairwise
from pathlib import Path
from threading import Lock, RLock, local
from types import MappingProxyType
from typing import Annotated, Self

from pydantic import (
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.live_contracts import ConsumerCursor, LiveChannel
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_PUBLICATION_LOCKS_GUARD = Lock()
_PUBLICATION_LOCKS: dict[str, RLock] = {}
_PUBLICATION_LOCK_STATE = local()
_SQLITE_CONNECT_IDENTITY_LOCK = Lock()
_FD_DIRECTORY_CANDIDATES = ("/proc/self/fd", "/dev/fd")
_FD_ATTESTATION_MAX_ENTRIES = 4096


def _open_descriptor_directory() -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    last_error: OSError | None = None
    for candidate in _FD_DIRECTORY_CANDIDATES:
        try:
            return os.open(candidate, flags)
        except OSError as exc:
            last_error = exc
    raise ReferenceDataIntegrityError(
        "platform has no trusted descriptor directory for SQLite attestation"
    ) from last_error


def _regular_descriptor_identities(
    *,
    max_entries: int = _FD_ATTESTATION_MAX_ENTRIES,
) -> Counter[tuple[int, int]]:
    if max_entries <= 0:
        raise ValueError("descriptor attestation max_entries must be positive")
    directory_descriptor = _open_descriptor_directory()
    identities: Counter[tuple[int, int]] = Counter()
    try:
        with os.scandir(directory_descriptor) as entries:
            for index, entry in enumerate(entries, start=1):
                if index > max_entries:
                    raise ReferenceDataIntegrityError(
                        "SQLite descriptor attestation entry limit exceeded"
                    )
                if not entry.name.isdigit():
                    continue
                descriptor = int(entry.name)
                try:
                    observed = os.fstat(descriptor)
                except OSError:
                    continue
                if stat.S_ISREG(observed.st_mode):
                    identities[(observed.st_dev, observed.st_ino)] += 1
        return identities
    finally:
        os.close(directory_descriptor)


class ReferenceDataset(StrEnum):
    """Reference domains shared by live decisions and historical replay."""

    ST_STATUS = "security_st_status"
    SUSPENSION_STATUS = "security_suspension_status"
    LISTING_STATUS = "security_listing_status"
    BOARD_MEMBERSHIP = "security_board_membership"
    ADJUSTMENT_FACTOR = "security_adjustment_factor"
    PRICE_LIMIT_REGIME = "security_price_limit_regime"


class ReferenceDataConflictError(RuntimeError):
    """An append or pointer transition conflicts with immutable history."""


class ReferenceDataIntegrityError(RuntimeError):
    """Persisted reference state cannot be trusted."""


class ReferenceDataUnavailableError(RuntimeError):
    """No unambiguous point-in-time value exists for a decision."""


class ReferencePublicationDeadlineError(RuntimeError):
    """A publication could not become durable before its hard deadline."""


class ReferencePublicationAuthenticationError(RuntimeError):
    """A cross-store publication proof is missing or cannot be authenticated."""


def _encode_time(value: datetime) -> str:
    return normalize_aware_utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decode_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ReferenceDataIntegrityError("stored reference timestamp is naive")
    return parsed.astimezone(UTC)


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def reference_publication_commit_intent_path(receipt_path: Path) -> Path:
    path = Path(receipt_path)
    return path.with_name(f"{path.stem}.intent.json")


class ReferencePublicationAuthenticator:
    """HMAC signer backed by an injected, service-isolated credential."""

    _ENVIRONMENT_PATH = "RQ_REFERENCE_PUBLICATION_HMAC_FILE"

    def __init__(self, *, key_id: str, secret: bytes) -> None:
        if (
            not key_id
            or len(key_id) > 128
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789-_." for character in key_id
            )
        ):
            raise ValueError("reference publication key_id is invalid")
        if len(secret) < 32:
            raise ValueError("reference publication HMAC secret must contain at least 32 bytes")
        self._key_id = key_id
        self._secret = bytes(secret)

    @property
    def key_id(self) -> str:
        return self._key_id

    @classmethod
    def from_file(cls, path: Path) -> ReferencePublicationAuthenticator:
        candidate = Path(os.path.abspath(path))
        descriptor = -1
        try:
            descriptor = os.open(
                candidate,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
            observed = os.fstat(descriptor)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.getuid()
                or observed.st_nlink != 1
                or stat.S_IMODE(observed.st_mode) & 0o077
            ):
                raise ReferencePublicationAuthenticationError(
                    "reference publication credential file is unsafe"
                )
            with os.fdopen(descriptor, "rb", closefd=True) as stream:
                descriptor = -1
                decoded = strict_canonical_json_loads(stream.read())
            if not isinstance(decoded, dict) or set(decoded) != {"key_id", "secret_hex"}:
                raise ValueError("credential payload fields are invalid")
            key_id = decoded["key_id"]
            secret_hex = decoded["secret_hex"]
            if not isinstance(key_id, str) or not isinstance(secret_hex, str):
                raise ValueError("credential payload values are invalid")
            secret = bytes.fromhex(secret_hex)
        except ReferencePublicationAuthenticationError:
            raise
        except (OSError, ValueError, TypeError) as exc:
            raise ReferencePublicationAuthenticationError(
                "reference publication credential is invalid"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        return cls(key_id=key_id, secret=secret)

    @classmethod
    def from_environment(cls) -> ReferencePublicationAuthenticator | None:
        configured = os.environ.get(cls._ENVIRONMENT_PATH)
        return None if not configured else cls.from_file(Path(configured))

    def sign(self, payload: Mapping[str, object]) -> str:
        return hmac.new(
            self._secret,
            bytes.fromhex(canonical_sha256(dict(payload))),
            hashlib.sha256,
        ).hexdigest()

    def verify(self, payload: Mapping[str, object], observed_mac: str) -> bool:
        return hmac.compare_digest(self.sign(payload), observed_mac)


class ReferenceRecord(RuntimeContractModel):
    """One immutable observation in a business-key revision lineage."""

    dataset_id: str = Field(min_length=1)
    key: str = Field(min_length=1)
    effective_from: AwareUtcDatetime
    effective_to: AwareUtcDatetime | None = None
    revision: int = Field(ge=1)
    source: str = Field(min_length=1)
    first_available_at: AwareUtcDatetime
    replacement_reason: str | None = Field(default=None, min_length=1)
    payload: Mapping[str, JsonValue] = Field(min_length=1)
    payload_sha256: Sha256 | str = ""
    record_id: Sha256 | str = ""

    @field_validator("payload", mode="after")
    @classmethod
    def canonicalize_payload(
        cls,
        value: Mapping[str, JsonValue],
    ) -> Mapping[str, JsonValue]:
        if any(not isinstance(key, str) or not key for key in value):
            raise ValueError("payload keys must be nonempty strings")
        copied = json.loads(_canonical_json(dict(value)))
        return MappingProxyType(dict(sorted(copied.items())))

    @field_serializer("payload")
    def serialize_payload(self, value: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        return dict(value)

    def identity_payload(self) -> dict[str, object]:
        return {
            "contract": "reference-record/v1",
            "dataset_id": self.dataset_id,
            "key": self.key,
            "effective_from": self.effective_from,
            "effective_to": self.effective_to,
            "revision": self.revision,
            "source": self.source,
            "first_available_at": self.first_available_at,
            "replacement_reason": self.replacement_reason,
            "payload_sha256": self.payload_sha256,
        }

    @model_validator(mode="after")
    def validate_record(self) -> Self:
        if self.effective_to is not None and self.effective_to <= self.effective_from:
            raise ValueError("effective_to must be after effective_from")
        if self.revision == 1 and self.replacement_reason is not None:
            raise ValueError("revision 1 cannot have replacement_reason")
        if self.revision > 1 and self.replacement_reason is None:
            raise ValueError("replacement_reason is required after revision 1")

        expected_payload_hash = canonical_sha256(self.payload)
        if self.payload_sha256 and self.payload_sha256 != expected_payload_hash:
            raise ValueError("payload_sha256 does not match payload")
        object.__setattr__(self, "payload_sha256", expected_payload_hash)

        expected_record_id = canonical_sha256(self.identity_payload())
        if self.record_id and self.record_id != expected_record_id:
            raise ValueError("record_id does not match immutable record content")
        object.__setattr__(self, "record_id", expected_record_id)
        return self


class ReferenceAppendResult(RuntimeContractModel):
    record: ReferenceRecord
    inserted: bool


class ReferenceGenerationManifest(RuntimeContractModel):
    schema_version: int = Field(default=2, ge=2)
    generation_id: Sha256 | str = ""
    previous_generation_id: Sha256 | None = None
    published_at: AwareUtcDatetime
    row_count: int = Field(ge=0)
    dataset_counts: Mapping[str, int]
    added_record_ids: tuple[Sha256, ...]
    content_sha256: Sha256
    manifest_sha256: Sha256 | str = ""

    @field_validator("dataset_counts", mode="after")
    @classmethod
    def canonicalize_counts(cls, value: Mapping[str, int]) -> Mapping[str, int]:
        if any(not key or count < 0 for key, count in value.items()):
            raise ValueError("dataset_counts must contain nonnegative named counts")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("dataset_counts")
    def serialize_counts(self, value: Mapping[str, int]) -> dict[str, int]:
        return dict(value)

    def generation_payload(self) -> dict[str, object]:
        return {
            "contract": "reference-generation/v2",
            "schema_version": self.schema_version,
            "previous_generation_id": self.previous_generation_id,
            "published_at": self.published_at,
            "row_count": self.row_count,
            "dataset_counts": self.dataset_counts,
            "added_record_ids": self.added_record_ids,
            "content_sha256": self.content_sha256,
        }

    def manifest_payload(self) -> dict[str, object]:
        return {**self.generation_payload(), "generation_id": self.generation_id}

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        if len(self.added_record_ids) != len(set(self.added_record_ids)):
            raise ValueError("added_record_ids must be unique")
        if tuple(sorted(self.added_record_ids)) != self.added_record_ids:
            raise ValueError("added_record_ids must be canonically sorted")
        if len(self.added_record_ids) > self.row_count:
            raise ValueError("added_record_ids cannot exceed cumulative row_count")
        if sum(self.dataset_counts.values()) != self.row_count:
            raise ValueError("dataset_counts do not sum to row_count")
        expected_generation = canonical_sha256(self.generation_payload())
        if self.generation_id and self.generation_id != expected_generation:
            raise ValueError("generation_id does not match manifest content")
        object.__setattr__(self, "generation_id", expected_generation)
        expected_manifest = canonical_sha256(self.manifest_payload())
        if self.manifest_sha256 and self.manifest_sha256 != expected_manifest:
            raise ValueError("manifest_sha256 does not match manifest content")
        object.__setattr__(self, "manifest_sha256", expected_manifest)
        return self


class ReferenceCurrentPointer(RuntimeContractModel):
    generation_id: Sha256
    manifest_sha256: Sha256
    switched_at: AwareUtcDatetime
    previous_generation_id: Sha256 | None = None

    @model_validator(mode="after")
    def validate_pointer(self) -> Self:
        if self.previous_generation_id == self.generation_id:
            raise ValueError("previous_generation_id must differ from generation_id")
        return self


class ReferencePublicationRollback(RuntimeContractModel):
    previous_pointer: ReferenceCurrentPointer | None = None
    created_generation_id: Sha256 | None = None
    inserted_record_ids: tuple[Sha256, ...] = ()

    @field_validator("inserted_record_ids")
    @classmethod
    def canonicalize_inserted_record_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("inserted_record_ids must be unique")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def validate_rollback(self) -> Self:
        if self.created_generation_id is None and self.inserted_record_ids:
            raise ValueError("inserted records require a created generation")
        return self


class ReferencePublicationCommitIntent(RuntimeContractModel):
    """Durable, but deliberately uncommitted, cross-store publication intent."""

    schema_version: int = 1
    publication_id: Sha256
    registry_generation_id: Sha256
    target_cursor: ConsumerCursor
    source_generation_id: Sha256
    channel: LiveChannel
    deadline: AwareUtcDatetime
    stage_sha256: Sha256
    key_id: str = Field(min_length=1, max_length=128)
    content_sha256: Sha256 | str = ""

    def content_payload(self) -> dict[str, object]:
        return {
            "contract": "reference-publication-commit-intent/v1",
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "registry_generation_id": self.registry_generation_id,
            "target_cursor": self.target_cursor,
            "source_generation_id": self.source_generation_id,
            "channel": self.channel,
            "deadline": self.deadline,
            "stage_sha256": self.stage_sha256,
            "key_id": self.key_id,
        }

    @model_validator(mode="after")
    def validate_intent(self) -> Self:
        if self.target_cursor.channel is not self.channel:
            raise ValueError("target cursor channel does not match commit intent")
        if self.target_cursor.source_generation_id != self.source_generation_id:
            raise ValueError("target cursor source generation does not match commit intent")
        expected = canonical_sha256(self.content_payload())
        if self.content_sha256 and self.content_sha256 != expected:
            raise ValueError("content_sha256 does not match commit intent")
        object.__setattr__(self, "content_sha256", expected)
        return self

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


class ReferencePublicationCompletionReceipt(RuntimeContractModel):
    """Strict cross-store commit marker shared by registry and cursor recovery."""

    schema_version: int = 1
    publication_id: Sha256
    registry_generation_id: Sha256
    target_cursor: ConsumerCursor
    source_generation_id: Sha256
    channel: LiveChannel
    deadline: AwareUtcDatetime
    durable_completed_at: AwareUtcDatetime
    intent_sha256: Sha256
    stage_sha256: Sha256
    key_id: str = Field(min_length=1, max_length=128)
    authentication_mac: Sha256
    content_sha256: Sha256 | str = ""

    def content_payload(self) -> dict[str, object]:
        return {
            "contract": "reference-publication-completion-receipt/v1",
            "schema_version": self.schema_version,
            "publication_id": self.publication_id,
            "registry_generation_id": self.registry_generation_id,
            "target_cursor": self.target_cursor,
            "source_generation_id": self.source_generation_id,
            "channel": self.channel,
            "deadline": self.deadline,
            "durable_completed_at": self.durable_completed_at,
            "intent_sha256": self.intent_sha256,
            "stage_sha256": self.stage_sha256,
            "key_id": self.key_id,
        }

    @model_validator(mode="after")
    def validate_receipt(self) -> Self:
        if self.target_cursor.channel is not self.channel:
            raise ValueError("target cursor channel does not match completion receipt")
        if self.target_cursor.source_generation_id != self.source_generation_id:
            raise ValueError("target cursor source generation does not match completion receipt")
        if self.durable_completed_at > self.deadline:
            raise ValueError("durable completion is after publication deadline")
        if self.durable_completed_at < self.target_cursor.updated_at:
            raise ValueError("durable completion precedes target cursor evidence")
        expected_intent = ReferencePublicationCommitIntent(
            publication_id=self.publication_id,
            registry_generation_id=self.registry_generation_id,
            target_cursor=self.target_cursor,
            source_generation_id=self.source_generation_id,
            channel=self.channel,
            deadline=self.deadline,
            stage_sha256=self.stage_sha256,
            key_id=self.key_id,
        )
        if self.intent_sha256 != expected_intent.content_sha256:
            raise ValueError("intent_sha256 does not match completion receipt")
        expected = canonical_sha256(self.content_payload())
        if self.content_sha256 and self.content_sha256 != expected:
            raise ValueError("content_sha256 does not match completion receipt")
        object.__setattr__(self, "content_sha256", expected)
        return self

    def authentication_payload(self) -> dict[str, object]:
        return {
            "contract": "reference-publication-completion-authentication/v1",
            **self.model_dump(
                mode="json",
                exclude={"authentication_mac"},
            ),
        }

    @classmethod
    def create_authenticated(
        cls,
        *,
        publication_id: str,
        registry_generation_id: str,
        target_cursor: ConsumerCursor,
        source_generation_id: str,
        channel: LiveChannel,
        deadline: datetime,
        durable_completed_at: datetime,
        intent_sha256: str,
        stage_sha256: str,
        authenticator: ReferencePublicationAuthenticator,
    ) -> ReferencePublicationCompletionReceipt:
        unsigned = cls(
            publication_id=publication_id,
            registry_generation_id=registry_generation_id,
            target_cursor=target_cursor,
            source_generation_id=source_generation_id,
            channel=channel,
            deadline=deadline,
            durable_completed_at=durable_completed_at,
            intent_sha256=intent_sha256,
            stage_sha256=stage_sha256,
            key_id=authenticator.key_id,
            authentication_mac="0" * 64,
        )
        return cls.model_validate(
            unsigned.model_dump(mode="python")
            | {"authentication_mac": authenticator.sign(unsigned.authentication_payload())}
        )

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


class ReferencePublicationDurableEvidence(RuntimeContractModel):
    schema_version: int = 1
    publication_id: Sha256
    stage_sha256: Sha256
    receipt_content_sha256: Sha256
    deadline: AwareUtcDatetime
    durable_completed_at: AwareUtcDatetime
    outcome: str = Field(pattern=r"^(committed|rolled_back_deadline)$")
    key_id: str = Field(min_length=1, max_length=128)
    authentication_mac: Sha256

    def authentication_payload(self) -> dict[str, object]:
        return {
            "contract": "reference-publication-durable-evidence/v1",
            **self.model_dump(mode="json", exclude={"authentication_mac"}),
        }

    @classmethod
    def create_authenticated(
        cls,
        *,
        receipt: ReferencePublicationCompletionReceipt,
        durable_completed_at: datetime,
        outcome: str,
        authenticator: ReferencePublicationAuthenticator,
    ) -> ReferencePublicationDurableEvidence:
        unsigned = cls(
            publication_id=receipt.publication_id,
            stage_sha256=receipt.stage_sha256,
            receipt_content_sha256=receipt.content_sha256,
            deadline=receipt.deadline,
            durable_completed_at=durable_completed_at,
            outcome=outcome,
            key_id=authenticator.key_id,
            authentication_mac="0" * 64,
        )
        return cls.model_validate(
            unsigned.model_dump(mode="python")
            | {"authentication_mac": authenticator.sign(unsigned.authentication_payload())}
        )


class _ReferencePublicationStage(RuntimeContractModel):
    schema_version: int = 1
    rollback: ReferencePublicationRollback
    append_results: tuple[ReferenceAppendResult, ...]
    manifest: ReferenceGenerationManifest
    not_after: AwareUtcDatetime
    publication_id: Sha256 | None = None
    completion_receipt_path: str | None = None
    target_cursor: ConsumerCursor | None = None

    @model_validator(mode="after")
    def validate_stage(self) -> Self:
        shared_fields = (
            self.publication_id,
            self.completion_receipt_path,
            self.target_cursor,
        )
        if (
            any(item is not None for item in shared_fields)
            and not all(item is not None for item in shared_fields)
            and (self.publication_id is None or self.completion_receipt_path is None)
        ):
            raise ValueError("shared publication stage is only partially bound")
        inserted_ids = tuple(
            result.record.record_id for result in self.append_results if result.inserted
        )
        if tuple(sorted(inserted_ids)) != self.rollback.inserted_record_ids:
            raise ValueError("staged append results do not match rollback token")
        if self.rollback.created_generation_id not in {
            None,
            self.manifest.generation_id,
        }:
            raise ValueError("staged manifest does not match rollback token")
        return self

    @property
    def stage_sha256(self) -> str:
        return canonical_sha256(
            {
                "contract": "reference-publication-stage/v1",
                "stage": self.model_dump(mode="python"),
            }
        )


class ReferencePendingPublication(RuntimeContractModel):
    rollback: ReferencePublicationRollback
    publication_id: Sha256
    completion_receipt_path: str
    target_cursor: ConsumerCursor
    receipt_is_committed: bool


class ReferenceLookup(RuntimeContractModel):
    record: ReferenceRecord
    generation_id: Sha256
    event_time: AwareUtcDatetime
    decision_time: AwareUtcDatetime


class ReferenceRegistry:
    """SQLite authority for immutable slow-reference revisions and generations."""

    _SCHEMA_VERSION = 2

    def __init__(
        self,
        path: Path | str,
        *,
        busy_timeout_ms: int = 5_000,
        publication_authenticator: ReferencePublicationAuthenticator | None = None,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(os.path.abspath(Path(path)))
        self._publication_lock_path = self.path.with_name(f".{self.path.name}.publication.lock")
        self.busy_timeout_ms = busy_timeout_ms
        self.publication_authenticator = (
            publication_authenticator or ReferencePublicationAuthenticator.from_environment()
        )
        self._database_identity: tuple[int, int] | None = None
        self._prepare_secure_parent(self.path.parent)
        self._initialize()
        with self.publication_commit_lock():
            self._recover_incomplete_publication()
        self._validate_integrity()

    @staticmethod
    def _prepare_secure_parent(path: Path) -> None:
        current = Path(path.anchor)
        for part in path.parts[1:]:
            current /= part
            try:
                observed = current.lstat()
            except FileNotFoundError:
                current.mkdir(mode=0o700)
                observed = current.lstat()
            if stat.S_ISLNK(observed.st_mode):
                raise ReferenceDataIntegrityError(
                    f"reference registry parent is a symlink: {current}"
                )
            if not stat.S_ISDIR(observed.st_mode):
                raise ReferenceDataIntegrityError(
                    f"reference registry parent is not a directory: {current}"
                )

    @staticmethod
    def _validate_database_file(path: Path, observed: os.stat_result) -> tuple[int, int]:
        if stat.S_ISLNK(observed.st_mode):
            raise ReferenceDataIntegrityError("reference registry path is a symlink")
        if not stat.S_ISREG(observed.st_mode):
            raise ReferenceDataIntegrityError("reference registry path is not a regular file")
        if observed.st_uid != os.geteuid():
            raise ReferenceDataIntegrityError("reference registry owner is unsafe")
        if observed.st_nlink != 1:
            raise ReferenceDataIntegrityError("reference registry link count is unsafe")
        if stat.S_IMODE(observed.st_mode) != 0o600:
            raise ReferenceDataIntegrityError("reference registry mode is unsafe")
        return observed.st_dev, observed.st_ino

    def _validate_sqlite_sidecars(self) -> frozenset[tuple[int, int]]:
        identities: set[tuple[int, int]] = set()
        for suffix in ("-wal", "-shm"):
            path = Path(f"{self.path}{suffix}")
            try:
                observed = path.lstat()
            except FileNotFoundError:
                continue
            identities.add(self._validate_database_file(path, observed))
        return frozenset(identities)

    @staticmethod
    def _attest_connected_database(
        *,
        identities_before_connect: Counter[tuple[int, int]],
        expected_main_identity: tuple[int, int],
    ) -> None:
        """Attest SQLite's main fd inside the dedicated registry process boundary."""

        identities_after_connect = _regular_descriptor_identities()
        if identities_before_connect[expected_main_identity] != 1:
            raise ReferenceDataIntegrityError(
                "registry process holds an untracked SQLite database descriptor"
            )
        if identities_after_connect[expected_main_identity] <= 1:
            raise ReferenceDataIntegrityError(
                "SQLite connected database identity is not the validated registry"
            )

    @contextmanager
    def publication_commit_lock(self, *, exclusive: bool = True) -> Iterator[None]:
        """Serialize the registry/cursor commit protocol across readers and writers."""

        lock_key = str(self._publication_lock_path)
        with _PUBLICATION_LOCKS_GUARD:
            thread_lock = _PUBLICATION_LOCKS.setdefault(lock_key, RLock())
        with thread_lock:
            held = getattr(_PUBLICATION_LOCK_STATE, "held", {})
            existing = held.get(lock_key)
            if existing is not None:
                descriptor, held_exclusive, depth = existing
                if exclusive and not held_exclusive:
                    raise ReferenceDataConflictError(
                        "cannot upgrade a shared reference publication lock"
                    )
                held[lock_key] = (descriptor, held_exclusive, depth + 1)
                try:
                    yield
                finally:
                    held[lock_key] = (descriptor, held_exclusive, depth)
                return

            descriptor = os.open(
                self._publication_lock_path,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                held[lock_key] = (descriptor, exclusive, 1)
                _PUBLICATION_LOCK_STATE.held = held
                yield
            finally:
                held.pop(lock_key, None)
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with _SQLITE_CONNECT_IDENTITY_LOCK, self._connect_locked() as connection:
            yield connection

    @contextmanager
    def _connect_locked(self) -> Iterator[sqlite3.Connection]:
        self._prepare_secure_parent(self.path.parent)
        database_descriptor = -1
        try:
            database_descriptor = os.open(
                self.path,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
            )
        except FileNotFoundError:
            try:
                database_descriptor = os.open(
                    self.path,
                    os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                )
            except OSError as exc:
                raise ReferenceDataIntegrityError(
                    "reference registry could not be created safely"
                ) from exc
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ReferenceDataIntegrityError("reference registry path is a symlink") from exc
            raise ReferenceDataIntegrityError(
                "reference registry could not be opened safely"
            ) from exc
        try:
            expected_identity = self._validate_database_file(
                self.path,
                os.fstat(database_descriptor),
            )
            if self._database_identity is not None and expected_identity != self._database_identity:
                raise ReferenceDataIntegrityError("reference registry identity changed")
            identities_before_connect = _regular_descriptor_identities()
            connection = sqlite3.connect(
                self.path,
                timeout=self.busy_timeout_ms / 1_000,
                isolation_level=None,
            )
            try:
                connection.row_factory = sqlite3.Row
                connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA synchronous = FULL")
                mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
                if str(mode).lower() != "wal":
                    raise ReferenceDataIntegrityError("reference registry requires WAL mode")
                connection.execute("PRAGMA schema_version").fetchone()
                path_identity = self._validate_database_file(
                    self.path,
                    self.path.lstat(),
                )
                if path_identity != expected_identity:
                    raise ReferenceDataIntegrityError(
                        "reference registry identity changed during connect"
                    )
                self._validate_sqlite_sidecars()
                self._attest_connected_database(
                    identities_before_connect=identities_before_connect,
                    expected_main_identity=expected_identity,
                )
                self._database_identity = expected_identity
                with connection:
                    yield connection
            finally:
                connection.close()
        finally:
            if database_descriptor >= 0:
                os.close(database_descriptor)

    def _initialize(self) -> None:
        with self._connect() as connection:
            mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            if str(mode).lower() != "wal":
                raise ReferenceDataIntegrityError("reference registry requires WAL mode")
            self._validate_sqlite_sidecars()
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS reference_metadata(
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS reference_record(
                        record_id TEXT PRIMARY KEY,
                        dataset_id TEXT NOT NULL,
                        business_key TEXT NOT NULL,
                        effective_from TEXT NOT NULL,
                        effective_to TEXT,
                        revision INTEGER NOT NULL CHECK(revision >= 1),
                        source TEXT NOT NULL,
                        first_available_at TEXT NOT NULL,
                        replacement_reason TEXT,
                        payload_json TEXT NOT NULL,
                        payload_sha256 TEXT NOT NULL,
                        UNIQUE(dataset_id, business_key, effective_from, revision)
                    );
                    CREATE INDEX IF NOT EXISTS reference_record_lookup
                    ON reference_record(dataset_id, business_key, first_available_at);
                    CREATE INDEX IF NOT EXISTS reference_record_available
                    ON reference_record(first_available_at, record_id);
                    CREATE TABLE IF NOT EXISTS reference_generation(
                        generation_id TEXT PRIMARY KEY,
                        previous_generation_id TEXT,
                        published_at TEXT NOT NULL,
                        row_count INTEGER NOT NULL,
                        dataset_counts_json TEXT NOT NULL,
                        content_sha256 TEXT NOT NULL,
                        manifest_json TEXT NOT NULL,
                        manifest_sha256 TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS reference_generation_member(
                        generation_id TEXT NOT NULL REFERENCES reference_generation(generation_id),
                        record_id TEXT NOT NULL REFERENCES reference_record(record_id),
                        PRIMARY KEY(generation_id, record_id)
                    );
                    CREATE INDEX IF NOT EXISTS reference_generation_member_record
                    ON reference_generation_member(record_id, generation_id);
                    CREATE TABLE IF NOT EXISTS reference_current(
                        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                        generation_id TEXT NOT NULL REFERENCES reference_generation(generation_id),
                        manifest_sha256 TEXT NOT NULL,
                        switched_at TEXT NOT NULL,
                        previous_generation_id TEXT
                    );
                    CREATE TABLE IF NOT EXISTS reference_publication_intent(
                        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                        rollback_json TEXT NOT NULL,
                        not_after TEXT NOT NULL,
                        publication_id TEXT,
                        completion_receipt_path TEXT,
                        stage_json TEXT
                    );
                    CREATE TABLE IF NOT EXISTS reference_publication_receipt(
                        generation_id TEXT PRIMARY KEY
                            REFERENCES reference_generation(generation_id),
                        completed_at TEXT NOT NULL,
                        visible_at TEXT NOT NULL
                    );
                    """
                )
                existing = connection.execute(
                    "SELECT value FROM reference_metadata WHERE key = 'schema_version'"
                ).fetchone()
                if existing is None:
                    connection.execute(
                        "INSERT INTO reference_metadata(key, value) VALUES ('schema_version', ?)",
                        (str(self._SCHEMA_VERSION),),
                    )
                elif existing["value"] != str(self._SCHEMA_VERSION):
                    raise ReferenceDataIntegrityError("unsupported reference registry schema")
                intent_columns = {
                    str(row["name"])
                    for row in connection.execute(
                        "PRAGMA table_info(reference_publication_intent)"
                    ).fetchall()
                }
                if "publication_id" not in intent_columns:
                    connection.execute(
                        "ALTER TABLE reference_publication_intent ADD COLUMN publication_id TEXT"
                    )
                if "completion_receipt_path" not in intent_columns:
                    connection.execute(
                        "ALTER TABLE reference_publication_intent "
                        "ADD COLUMN completion_receipt_path TEXT"
                    )
                if "stage_json" not in intent_columns:
                    connection.execute(
                        "ALTER TABLE reference_publication_intent ADD COLUMN stage_json TEXT"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _recover_incomplete_publication(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT rollback_json, publication_id, completion_receipt_path, stage_json "
                    "FROM reference_publication_intent "
                    "WHERE singleton = 1"
                ).fetchone()
                if row is not None:
                    try:
                        rollback = ReferencePublicationRollback.model_validate_json(
                            row["rollback_json"]
                        )
                    except ValueError as exc:
                        raise ReferenceDataIntegrityError(
                            "reference publication intent is invalid"
                        ) from exc
                    stage = self._stage_from_row(row)
                    if stage is None or stage.publication_id is None:
                        generation_exists = False
                        if rollback.created_generation_id is not None:
                            generation_exists = (
                                connection.execute(
                                    "SELECT 1 FROM reference_generation WHERE generation_id = ?",
                                    (rollback.created_generation_id,),
                                ).fetchone()
                                is not None
                            )
                        if generation_exists:
                            self._compensate_publication_in_connection(connection, rollback)
                        connection.execute(
                            "DELETE FROM reference_publication_intent WHERE singleton = 1"
                        )
                    else:
                        receipt = self._validated_completion_receipt(stage, strict=False)
                        if receipt is not None:
                            self._commit_stage_in_connection(
                                connection,
                                stage,
                                durable_completed_at=receipt.durable_completed_at,
                            )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _stage_from_row(row: sqlite3.Row) -> _ReferencePublicationStage | None:
        if row["stage_json"] is None:
            return None
        try:
            return _ReferencePublicationStage.model_validate_json(row["stage_json"])
        except ValueError as exc:
            raise ReferenceDataIntegrityError("reference publication stage is invalid") from exc

    @staticmethod
    def _load_completion_receipt(path: Path) -> ReferencePublicationCompletionReceipt:
        try:
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.getuid()
                or stat.S_IMODE(observed.st_mode) != 0o600
            ):
                raise ReferenceDataIntegrityError("publication completion receipt is unsafe")
            decoded = strict_canonical_json_loads(path.read_bytes())
            return ReferencePublicationCompletionReceipt.model_validate(decoded)
        except ReferenceDataIntegrityError:
            raise
        except (OSError, ValueError, TypeError) as exc:
            raise ReferenceDataIntegrityError("publication completion receipt is invalid") from exc

    @staticmethod
    def _load_completion_evidence(path: Path) -> ReferencePublicationDurableEvidence:
        try:
            observed = path.lstat()
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.getuid()
                or observed.st_nlink != 1
                or stat.S_IMODE(observed.st_mode) != 0o600
            ):
                raise ReferenceDataIntegrityError("publication completion evidence is unsafe")
            decoded = strict_canonical_json_loads(path.read_bytes())
            return ReferencePublicationDurableEvidence.model_validate(decoded)
        except ReferenceDataIntegrityError:
            raise
        except (OSError, ValueError, TypeError) as exc:
            raise ReferenceDataIntegrityError("publication completion evidence is invalid") from exc

    def _completion_receipt_matches(
        self,
        *,
        stage: _ReferencePublicationStage,
    ) -> bool:
        return self._validated_completion_receipt(stage, strict=False) is not None

    def _validated_completion_receipt(
        self,
        stage: _ReferencePublicationStage,
        *,
        strict: bool,
    ) -> ReferencePublicationCompletionReceipt | None:
        if (
            stage.publication_id is None
            or stage.completion_receipt_path is None
            or stage.target_cursor is None
        ):
            if strict:
                raise ReferenceDataIntegrityError(
                    "publication completion receipt is not fully bound"
                )
            return None
        path = Path(stage.completion_receipt_path)
        intent_path = reference_publication_commit_intent_path(path)
        if intent_path.exists() or not path.exists():
            if strict:
                raise ReferenceDataIntegrityError(
                    "publication completion receipt is uncommitted or missing"
                )
            return None
        try:
            receipt = self._load_completion_receipt(path)
        except ReferenceDataIntegrityError:
            if strict:
                raise
            return None
        matches = (
            receipt.publication_id == stage.publication_id
            and receipt.registry_generation_id == stage.manifest.generation_id
            and receipt.target_cursor == stage.target_cursor
            and receipt.source_generation_id == stage.target_cursor.source_generation_id
            and receipt.channel is stage.target_cursor.channel
            and receipt.deadline == stage.not_after
            and receipt.stage_sha256 == stage.stage_sha256
        )
        if not matches:
            if strict:
                raise ReferenceDataIntegrityError(
                    "publication completion receipt does not match staged publication"
                )
            return None
        authenticator = self.publication_authenticator
        authenticated = (
            authenticator is not None
            and receipt.key_id == authenticator.key_id
            and authenticator.verify(
                receipt.authentication_payload(),
                receipt.authentication_mac,
            )
        )
        if not authenticated:
            if strict:
                raise ReferenceDataIntegrityError(
                    "publication completion receipt authentication failed"
                )
            return None
        evidence_path = path.parent.parent / "completion-evidence" / path.name
        if not evidence_path.exists():
            if strict:
                raise ReferenceDataIntegrityError(
                    "publication durable completion evidence is missing"
                )
            return None
        try:
            evidence = self._load_completion_evidence(evidence_path)
        except ReferenceDataIntegrityError:
            if strict:
                raise
            return None
        evidence_matches = (
            evidence.publication_id == receipt.publication_id
            and evidence.stage_sha256 == receipt.stage_sha256
            and evidence.receipt_content_sha256 == receipt.content_sha256
            and evidence.deadline == receipt.deadline
            and evidence.outcome == "committed"
            and evidence.durable_completed_at <= receipt.deadline
            and authenticator is not None
            and evidence.key_id == authenticator.key_id
            and authenticator.verify(
                evidence.authentication_payload(),
                evidence.authentication_mac,
            )
        )
        if not evidence_matches:
            if strict:
                raise ReferenceDataIntegrityError(
                    "publication durable completion evidence is invalid"
                )
            return None
        return receipt

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> ReferenceRecord:
        try:
            return ReferenceRecord(
                record_id=row["record_id"],
                dataset_id=row["dataset_id"],
                key=row["business_key"],
                effective_from=_decode_time(row["effective_from"]),
                effective_to=_decode_time(row["effective_to"]),
                revision=int(row["revision"]),
                source=row["source"],
                first_available_at=_decode_time(row["first_available_at"]),
                replacement_reason=row["replacement_reason"],
                payload=json.loads(row["payload_json"]),
                payload_sha256=row["payload_sha256"],
            )
        except Exception as exc:
            raise ReferenceDataIntegrityError("stored reference record is invalid") from exc

    @staticmethod
    def _manifest_from_row(row: sqlite3.Row) -> ReferenceGenerationManifest:
        try:
            manifest = ReferenceGenerationManifest.model_validate_json(row["manifest_json"])
        except Exception as exc:
            raise ReferenceDataIntegrityError("stored generation manifest is invalid") from exc
        columns = (
            row["generation_id"],
            row["previous_generation_id"],
            row["published_at"],
            int(row["row_count"]),
            row["dataset_counts_json"],
            row["content_sha256"],
            row["manifest_sha256"],
        )
        expected = (
            manifest.generation_id,
            manifest.previous_generation_id,
            _encode_time(manifest.published_at),
            manifest.row_count,
            _canonical_json(dict(manifest.dataset_counts)),
            manifest.content_sha256,
            manifest.manifest_sha256,
        )
        if columns != expected:
            raise ReferenceDataIntegrityError("generation manifest hash or columns mismatch")
        return manifest

    def _validate_integrity(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN")
            try:
                self._validate_integrity_in_connection(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _validate_integrity_in_connection(self, connection: sqlite3.Connection) -> None:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise ReferenceDataIntegrityError("reference registry integrity_check failed")
        record_rows = connection.execute(
            "SELECT * FROM reference_record ORDER BY dataset_id, business_key, "
            "effective_from, revision"
        ).fetchall()
        records = tuple(self._record_from_row(row) for row in record_rows)
        self._validate_revision_history(records)
        rows = connection.execute(
            """
            WITH RECURSIVE ancestry(generation_id, depth) AS (
                SELECT generation_id, 0 FROM reference_generation
                WHERE previous_generation_id IS NULL
                UNION ALL
                SELECT child.generation_id, parent.depth + 1
                FROM reference_generation AS child
                JOIN ancestry AS parent
                  ON child.previous_generation_id = parent.generation_id
            )
            SELECT generation.* FROM reference_generation AS generation
            JOIN ancestry ON ancestry.generation_id = generation.generation_id
            ORDER BY ancestry.depth, generation.generation_id
            """
        ).fetchall()
        generation_count = int(
            connection.execute("SELECT COUNT(*) FROM reference_generation").fetchone()[0]
        )
        if len(rows) != generation_count:
            raise ReferenceDataIntegrityError("generation ancestry is cyclic or orphaned")
        validated_generations: set[str] = set()
        for row in rows:
            manifest = self._manifest_from_row(row)
            member_rows = connection.execute(
                """
                SELECT record_id FROM reference_generation_member
                WHERE generation_id = ? ORDER BY record_id
                """,
                (manifest.generation_id,),
            ).fetchall()
            added_ids = tuple(str(member_row["record_id"]) for member_row in member_rows)
            if added_ids != manifest.added_record_ids:
                raise ReferenceDataIntegrityError("generation delta membership hash mismatch")
            if (
                manifest.previous_generation_id is not None
                and manifest.previous_generation_id not in validated_generations
            ):
                raise ReferenceDataIntegrityError("generation parent is missing or unordered")
            parent_manifest = (
                self._generation_in_connection(connection, manifest.previous_generation_id)
                if manifest.previous_generation_id is not None
                else None
            )
            if parent_manifest is not None:
                repeated = connection.execute(
                    """
                    WITH RECURSIVE ancestry(generation_id, previous_generation_id) AS (
                        SELECT generation_id, previous_generation_id
                        FROM reference_generation WHERE generation_id = ?
                        UNION ALL
                        SELECT parent.generation_id, parent.previous_generation_id
                        FROM reference_generation AS parent
                        JOIN ancestry AS child
                          ON parent.generation_id = child.previous_generation_id
                    )
                    SELECT 1 FROM reference_generation_member AS added
                    JOIN reference_generation_member AS inherited
                      ON inherited.record_id = added.record_id
                    JOIN ancestry AS generation
                      ON generation.generation_id = inherited.generation_id
                    WHERE added.generation_id = ?
                    LIMIT 1
                    """,
                    (parent_manifest.generation_id, manifest.generation_id),
                ).fetchone()
                if repeated is not None:
                    raise ReferenceDataIntegrityError(
                        "generation delta repeats an inherited member"
                    )
            expected_row_count = (
                parent_manifest.row_count if parent_manifest is not None else 0
            ) + len(added_ids)
            if expected_row_count != manifest.row_count:
                raise ReferenceDataIntegrityError("generation cumulative row_count mismatch")
            parent_content_sha256 = (
                parent_manifest.content_sha256 if parent_manifest is not None else None
            )
            expected_content_sha256 = canonical_sha256(
                {
                    "contract": "reference-generation-content/v2",
                    "parent_content_sha256": parent_content_sha256,
                    "added_record_ids": added_ids,
                }
            )
            if expected_content_sha256 != manifest.content_sha256:
                raise ReferenceDataIntegrityError("generation cumulative content hash mismatch")
            added_records = tuple(
                self._record_from_row(member_row)
                for member_row in connection.execute(
                    """
                    SELECT record.* FROM reference_record AS record
                    JOIN reference_generation_member AS member
                      ON member.record_id = record.record_id
                    WHERE member.generation_id = ?
                    ORDER BY record.record_id
                    """,
                    (manifest.generation_id,),
                ).fetchall()
            )
            member_counts: Counter[str] = Counter()
            if parent_manifest is not None:
                member_counts.update(dict(parent_manifest.dataset_counts))
            member_counts.update(record.dataset_id for record in added_records)
            member_counts = dict(member_counts)
            if member_counts != dict(manifest.dataset_counts):
                raise ReferenceDataIntegrityError("generation dataset counts mismatch")
            validated_generations.add(manifest.generation_id)
        pointer_row = connection.execute(
            "SELECT * FROM reference_current WHERE singleton = 1"
        ).fetchone()
        if pointer_row is not None:
            pointer = self._pointer_from_row(pointer_row)
            manifest = self._generation_in_connection(connection, pointer.generation_id)
            if pointer.manifest_sha256 != manifest.manifest_sha256:
                raise ReferenceDataIntegrityError("current pointer manifest hash mismatch")
        receipt_rows = connection.execute(
            """
            SELECT receipt.completed_at, receipt.visible_at, generation.published_at
            FROM reference_publication_receipt AS receipt
            JOIN reference_generation AS generation
              ON generation.generation_id = receipt.generation_id
            """
        ).fetchall()
        for receipt_row in receipt_rows:
            completed_at = _decode_time(receipt_row["completed_at"])
            visible_at = _decode_time(receipt_row["visible_at"])
            published_at = _decode_time(receipt_row["published_at"])
            if (
                completed_at is None
                or visible_at is None
                or published_at is None
                or completed_at > visible_at
                or visible_at != published_at
            ):
                raise ReferenceDataIntegrityError(
                    "reference publication completion receipt is invalid"
                )

    @classmethod
    def _validate_revision_history(cls, records: tuple[ReferenceRecord, ...]) -> None:
        grouped: dict[tuple[str, str], list[ReferenceRecord]] = {}
        for record in records:
            grouped.setdefault((record.dataset_id, record.key), []).append(record)
        for business_records in grouped.values():
            lineages: dict[datetime, list[ReferenceRecord]] = {}
            for record in business_records:
                lineages.setdefault(record.effective_from, []).append(record)
            latest: list[ReferenceRecord] = []
            for lineage in lineages.values():
                ordered = sorted(lineage, key=lambda record: record.revision)
                expected_revisions = list(range(1, len(ordered) + 1))
                if [record.revision for record in ordered] != expected_revisions:
                    raise ReferenceDataIntegrityError("reference revision history has a gap")
                if any(
                    later.first_available_at < earlier.first_available_at
                    for earlier, later in pairwise(ordered)
                ):
                    raise ReferenceDataIntegrityError(
                        "reference revision availability moves backwards"
                    )
                latest.append(ordered[-1])
            for index, first in enumerate(latest):
                for second in latest[index + 1 :]:
                    if cls._periods_overlap(first, second):
                        raise ReferenceDataIntegrityError("overlapping effective reference values")

    @staticmethod
    def _pointer_from_row(row: sqlite3.Row) -> ReferenceCurrentPointer:
        try:
            return ReferenceCurrentPointer(
                generation_id=row["generation_id"],
                manifest_sha256=row["manifest_sha256"],
                switched_at=_decode_time(row["switched_at"]),
                previous_generation_id=row["previous_generation_id"],
            )
        except Exception as exc:
            raise ReferenceDataIntegrityError("stored current pointer is invalid") from exc

    def append(self, record: ReferenceRecord) -> ReferenceAppendResult:
        validated = ReferenceRecord.model_validate(record)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._fail_closed_on_pending_write(connection)
                result = self._append_many_in_connection(connection, (validated,))[0]
                connection.commit()
                return result
            except BaseException:
                connection.rollback()
                raise

    def _append_many_in_connection(
        self,
        connection: sqlite3.Connection,
        records: tuple[ReferenceRecord, ...],
        *,
        persist: bool = True,
    ) -> tuple[ReferenceAppendResult, ...]:
        lineage_periods: dict[tuple[str, str, datetime], datetime | None] = {}
        for record in records:
            lineage_key = (record.dataset_id, record.key, record.effective_from)
            previous_effective_to = lineage_periods.setdefault(lineage_key, record.effective_to)
            if previous_effective_to != record.effective_to:
                raise ReferenceDataConflictError(
                    "one batch cannot assign different periods to the same lineage"
                )
        connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS incoming_reference_lineage(
                dataset_id TEXT NOT NULL,
                business_key TEXT NOT NULL,
                effective_from TEXT NOT NULL,
                effective_to TEXT,
                PRIMARY KEY(dataset_id, business_key, effective_from)
            ) WITHOUT ROWID
            """
        )
        connection.execute("DELETE FROM incoming_reference_lineage")
        connection.executemany(
            """
            INSERT INTO incoming_reference_lineage(
                dataset_id, business_key, effective_from, effective_to
            ) VALUES (?, ?, ?, ?)
            """,
            (
                (
                    dataset_id,
                    key,
                    _encode_time(effective_from),
                    _encode_time(effective_to) if effective_to is not None else None,
                )
                for (dataset_id, key, effective_from), effective_to in lineage_periods.items()
            ),
        )
        existing_rows = connection.execute(
            """
            SELECT DISTINCT record.* FROM reference_record AS record
            JOIN incoming_reference_lineage AS incoming
              ON incoming.dataset_id = record.dataset_id
             AND incoming.business_key = record.business_key
            WHERE record.effective_from = incoming.effective_from
               OR (
                    (incoming.effective_to IS NULL
                     OR record.effective_from < incoming.effective_to)
                AND (record.effective_to IS NULL
                     OR incoming.effective_from < record.effective_to)
               )
            ORDER BY record.dataset_id, record.business_key,
                     record.effective_from, record.revision
            """
        ).fetchall()
        existing_records = [self._record_from_row(row) for row in existing_rows]
        lineages: dict[tuple[str, str, datetime], list[ReferenceRecord]] = {}
        business_lineages: dict[tuple[str, str], dict[datetime, ReferenceRecord]] = {}
        for existing in existing_records:
            lineage_key = (existing.dataset_id, existing.key, existing.effective_from)
            lineages.setdefault(lineage_key, []).append(existing)
            business_lineages.setdefault((existing.dataset_id, existing.key), {})[
                existing.effective_from
            ] = existing

        inserted: list[ReferenceRecord] = []
        results: list[ReferenceAppendResult] = []
        for validated in records:
            lineage_key = (validated.dataset_id, validated.key, validated.effective_from)
            lineage = lineages.setdefault(lineage_key, [])
            exact = next(
                (item for item in lineage if item.revision == validated.revision),
                None,
            )
            if exact is not None:
                if exact != validated:
                    raise ReferenceDataConflictError(
                        f"revision {validated.revision} already has different content"
                    )
                results.append(ReferenceAppendResult(record=exact, inserted=False))
                continue
            if lineage:
                previous = lineage[-1]
                if validated.revision != previous.revision + 1:
                    raise ReferenceDataConflictError(
                        f"next revision must be {previous.revision + 1}"
                    )
                if validated.first_available_at < previous.first_available_at:
                    raise ReferenceDataConflictError(
                        "first_available_at cannot regress across revisions"
                    )
            elif validated.revision != 1:
                raise ReferenceDataConflictError("new lineage must start at revision 1")

            latest_by_effective = business_lineages.setdefault(
                (validated.dataset_id, validated.key), {}
            )
            for effective_from, existing in latest_by_effective.items():
                if effective_from != validated.effective_from and self._periods_overlap(
                    validated, existing
                ):
                    raise ReferenceDataConflictError(
                        "effective period overlap with lineage "
                        f"{existing.effective_from.isoformat()}"
                    )
            lineage.append(validated)
            latest_by_effective[validated.effective_from] = validated
            inserted.append(validated)
            results.append(ReferenceAppendResult(record=validated, inserted=True))

        if persist:
            self._insert_records_in_connection(connection, tuple(inserted))
        return tuple(results)

    @staticmethod
    def _insert_records_in_connection(
        connection: sqlite3.Connection,
        records: tuple[ReferenceRecord, ...],
    ) -> None:
        connection.executemany(
            """
            INSERT INTO reference_record(
                record_id, dataset_id, business_key, effective_from, effective_to,
                revision, source, first_available_at, replacement_reason,
                payload_json, payload_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                (
                    record.record_id,
                    record.dataset_id,
                    record.key,
                    _encode_time(record.effective_from),
                    _encode_time(record.effective_to) if record.effective_to else None,
                    record.revision,
                    record.source,
                    _encode_time(record.first_available_at),
                    record.replacement_reason,
                    _canonical_json(dict(record.payload)),
                    record.payload_sha256,
                )
                for record in records
            ),
        )

    def append_many_and_publish(
        self,
        records: tuple[ReferenceRecord, ...],
        *,
        published_at: datetime,
    ) -> tuple[tuple[ReferenceAppendResult, ...], ReferenceGenerationManifest]:
        validated = tuple(ReferenceRecord.model_validate(record) for record in records)
        observed = normalize_aware_utc(published_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._fail_closed_on_pending_write(connection)
                results = self._append_many_in_connection(connection, validated)
                manifest = self._publish_in_connection(connection, observed)
                connection.commit()
                return results, manifest
            except BaseException:
                connection.rollback()
                raise

    def append_many_and_publish_before(
        self,
        records: tuple[ReferenceRecord, ...],
        *,
        published_at: datetime,
        completion_clock: Callable[[], datetime],
        not_after: datetime,
        retain_intent: bool = False,
        publication_id: str | None = None,
        completion_receipt_path: Path | None = None,
        target_cursor: ConsumerCursor | None = None,
    ) -> tuple[
        tuple[ReferenceAppendResult, ...],
        ReferenceGenerationManifest,
        ReferencePublicationRollback,
    ]:
        validated = tuple(ReferenceRecord.model_validate(record) for record in records)
        observed = normalize_aware_utc(published_at)
        deadline = normalize_aware_utc(not_after)
        if (publication_id is None) != (completion_receipt_path is None):
            raise ValueError("publication_id and completion_receipt_path must be provided together")
        if target_cursor is not None and publication_id is None:
            raise ValueError("target_cursor requires a shared publication_id")
        if observed > deadline:
            raise ReferencePublicationDeadlineError("publication started after deadline")
        if any(record.first_available_at != observed for record in validated):
            raise ReferenceDataConflictError("deadline publication records must share published_at")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                pending_row = connection.execute(
                    "SELECT 1 FROM reference_publication_intent WHERE singleton = 1"
                ).fetchone()
                if pending_row is not None:
                    raise ReferenceDataConflictError("reference publication is already pending")
                previous_row = connection.execute(
                    "SELECT * FROM reference_current WHERE singleton = 1"
                ).fetchone()
                previous = (
                    self._pointer_from_row(previous_row) if previous_row is not None else None
                )
                results = self._append_many_in_connection(
                    connection,
                    validated,
                    persist=False,
                )
                manifest = self._build_staged_manifest(
                    connection,
                    results=results,
                    published_at=observed,
                    previous=previous,
                )
                created_generation_id = (
                    manifest.generation_id
                    if previous is None or manifest.generation_id != previous.generation_id
                    else None
                )
                inserted_record_ids = tuple(
                    result.record.record_id for result in results if result.inserted
                )
                rollback = ReferencePublicationRollback(
                    previous_pointer=previous,
                    created_generation_id=created_generation_id,
                    inserted_record_ids=inserted_record_ids,
                )
                stage = _ReferencePublicationStage(
                    rollback=rollback,
                    append_results=results,
                    manifest=manifest,
                    not_after=deadline,
                    publication_id=publication_id,
                    completion_receipt_path=(
                        str(completion_receipt_path) if completion_receipt_path else None
                    ),
                    target_cursor=target_cursor,
                )
                connection.execute(
                    """
                    INSERT INTO reference_publication_intent(
                        singleton, rollback_json, not_after,
                        publication_id, completion_receipt_path, stage_json
                    ) VALUES (1, ?, ?, ?, ?, ?)
                    """,
                    (
                        rollback.model_dump_json(),
                        _encode_time(deadline),
                        publication_id,
                        str(completion_receipt_path) if completion_receipt_path else None,
                        stage.model_dump_json(),
                    ),
                )
                before_commit = normalize_aware_utc(completion_clock())
                if before_commit > deadline:
                    connection.rollback()
                    raise ReferencePublicationDeadlineError("publication completed after deadline")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            completed = normalize_aware_utc(completion_clock())
            if completed > deadline or completed > observed:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    connection.execute(
                        "DELETE FROM reference_publication_intent WHERE singleton = 1"
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
                raise ReferencePublicationDeadlineError("publication completed after deadline")

            if not retain_intent:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    self._commit_stage_in_connection(
                        connection,
                        stage,
                        durable_completed_at=completed,
                    )
                    connection.execute(
                        "DELETE FROM reference_publication_intent WHERE singleton = 1"
                    )
                    connection.commit()
                except BaseException:
                    connection.rollback()
                    raise
        return results, manifest, rollback

    def _build_staged_manifest(
        self,
        connection: sqlite3.Connection,
        *,
        results: tuple[ReferenceAppendResult, ...],
        published_at: datetime,
        previous: ReferenceCurrentPointer | None,
    ) -> ReferenceGenerationManifest:
        inserted = tuple(result.record for result in results if result.inserted)
        if previous is not None and not inserted:
            return self._generation_in_connection(connection, previous.generation_id)
        parent = (
            self._generation_in_connection(connection, previous.generation_id)
            if previous is not None
            else None
        )
        added_record_ids = tuple(sorted(record.record_id for record in inserted))
        counts: Counter[str] = Counter()
        if parent is not None:
            counts.update(dict(parent.dataset_counts))
        counts.update(record.dataset_id for record in inserted)
        return ReferenceGenerationManifest(
            previous_generation_id=previous.generation_id if previous is not None else None,
            published_at=published_at,
            row_count=(parent.row_count if parent is not None else 0) + len(inserted),
            dataset_counts=dict(counts),
            added_record_ids=added_record_ids,
            content_sha256=canonical_sha256(
                {
                    "contract": "reference-generation-content/v2",
                    "parent_content_sha256": parent.content_sha256 if parent is not None else None,
                    "added_record_ids": added_record_ids,
                }
            ),
        )

    def _commit_stage_in_connection(
        self,
        connection: sqlite3.Connection,
        stage: _ReferencePublicationStage,
        *,
        durable_completed_at: datetime | None = None,
    ) -> None:
        previous_row = connection.execute(
            "SELECT * FROM reference_current WHERE singleton = 1"
        ).fetchone()
        observed_previous = (
            self._pointer_from_row(previous_row) if previous_row is not None else None
        )
        manifest = stage.manifest
        if (
            stage.rollback.created_generation_id is not None
            and observed_previous is not None
            and observed_previous.generation_id == manifest.generation_id
        ):
            persisted = self._generation_in_connection(connection, manifest.generation_id)
            if (
                persisted != manifest
                or observed_previous.manifest_sha256 != manifest.manifest_sha256
                or observed_previous.previous_generation_id != manifest.previous_generation_id
            ):
                raise ReferenceDataIntegrityError(
                    "staged reference generation does not match durable state"
                )
            receipt_row = connection.execute(
                "SELECT 1 FROM reference_publication_receipt WHERE generation_id = ?",
                (manifest.generation_id,),
            ).fetchone()
            if receipt_row is None:
                raise ReferenceDataIntegrityError(
                    "staged reference generation is missing its durable receipt"
                )
            return
        if observed_previous != stage.rollback.previous_pointer:
            raise ReferenceDataConflictError(
                "reference current changed while publication was staged"
            )
        if stage.rollback.created_generation_id is None:
            return
        inserted_records = tuple(
            result.record for result in stage.append_results if result.inserted
        )
        self._insert_records_in_connection(connection, inserted_records)
        connection.execute(
            """
            INSERT INTO reference_generation(
                generation_id, previous_generation_id, published_at, row_count,
                dataset_counts_json, content_sha256, manifest_json, manifest_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                manifest.generation_id,
                manifest.previous_generation_id,
                _encode_time(manifest.published_at),
                manifest.row_count,
                _canonical_json(dict(manifest.dataset_counts)),
                manifest.content_sha256,
                manifest.model_dump_json(),
                manifest.manifest_sha256,
            ),
        )
        connection.executemany(
            "INSERT INTO reference_generation_member(generation_id, record_id) VALUES (?, ?)",
            ((manifest.generation_id, record_id) for record_id in manifest.added_record_ids),
        )
        connection.execute(
            """
            INSERT INTO reference_current(
                singleton, generation_id, manifest_sha256, switched_at,
                previous_generation_id
            ) VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(singleton) DO UPDATE SET
                generation_id = excluded.generation_id,
                manifest_sha256 = excluded.manifest_sha256,
                switched_at = excluded.switched_at,
                previous_generation_id = excluded.previous_generation_id
            """,
            (
                manifest.generation_id,
                manifest.manifest_sha256,
                _encode_time(manifest.published_at),
                manifest.previous_generation_id,
            ),
        )
        completed = normalize_aware_utc(durable_completed_at or manifest.published_at)
        if completed > manifest.published_at:
            raise ReferencePublicationDeadlineError(
                "durable completion is after staged visibility horizon"
            )
        connection.execute(
            """
            INSERT INTO reference_publication_receipt(
                generation_id, completed_at, visible_at
            ) VALUES (?, ?, ?)
            """,
            (
                manifest.generation_id,
                _encode_time(completed),
                _encode_time(manifest.published_at),
            ),
        )

    def compensate_publication(self, rollback: ReferencePublicationRollback) -> None:
        token = ReferencePublicationRollback.model_validate(rollback)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT rollback_json, stage_json FROM reference_publication_intent "
                    "WHERE singleton = 1"
                ).fetchone()
                if row is not None:
                    pending = ReferencePublicationRollback.model_validate_json(row["rollback_json"])
                    if pending != token:
                        raise ReferenceDataConflictError(
                            "publication compensation token does not match pending intent"
                        )
                    generation_exists = False
                    if token.created_generation_id is not None:
                        generation_exists = (
                            connection.execute(
                                "SELECT 1 FROM reference_generation WHERE generation_id = ?",
                                (token.created_generation_id,),
                            ).fetchone()
                            is not None
                        )
                    if row["stage_json"] is None or generation_exists:
                        self._compensate_publication_in_connection(connection, token)
                connection.execute("DELETE FROM reference_publication_intent WHERE singleton = 1")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def pending_publication_stage_sha256(
        self,
        rollback: ReferencePublicationRollback,
    ) -> str:
        """Return the immutable stage identity bound to a pending publication."""

        token = ReferencePublicationRollback.model_validate(rollback)
        with self._connect() as connection:
            row = connection.execute(
                "SELECT rollback_json, publication_id, completion_receipt_path, stage_json "
                "FROM reference_publication_intent WHERE singleton = 1"
            ).fetchone()
            if row is None:
                raise ReferenceDataConflictError("reference publication is not pending")
            pending = ReferencePublicationRollback.model_validate_json(row["rollback_json"])
            if pending != token:
                raise ReferenceDataConflictError(
                    "publication stage token does not match pending intent"
                )
            stage = self._stage_from_row(row)
            if stage is None:
                raise ReferenceDataIntegrityError(
                    "pending publication does not contain a staged generation"
                )
            return stage.stage_sha256

    def pending_publication(self) -> ReferencePendingPublication | None:
        """Inspect a shared pending publication while holding the commit lock."""

        with self._connect() as connection:
            row = connection.execute(
                "SELECT rollback_json, publication_id, completion_receipt_path, stage_json "
                "FROM reference_publication_intent WHERE singleton = 1"
            ).fetchone()
            if row is None:
                return None
            stage = self._stage_from_row(row)
            if (
                stage is None
                or stage.publication_id is None
                or stage.completion_receipt_path is None
                or stage.target_cursor is None
            ):
                raise ReferenceDataIntegrityError(
                    "pending shared publication is incompletely bound"
                )
            rollback = ReferencePublicationRollback.model_validate_json(row["rollback_json"])
            if rollback != stage.rollback:
                raise ReferenceDataIntegrityError(
                    "pending shared publication rollback token changed"
                )
            return ReferencePendingPublication(
                rollback=rollback,
                publication_id=stage.publication_id,
                completion_receipt_path=stage.completion_receipt_path,
                target_cursor=stage.target_cursor,
                receipt_is_committed=(
                    self._validated_completion_receipt(stage, strict=False) is not None
                ),
            )

    def commit_publication_stage(self, rollback: ReferencePublicationRollback) -> None:
        """Commit staged registry rows while retaining the fail-closed intent."""

        token = ReferencePublicationRollback.model_validate(rollback)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT rollback_json, publication_id, completion_receipt_path, stage_json "
                    "FROM reference_publication_intent "
                    "WHERE singleton = 1"
                ).fetchone()
                if row is None:
                    return
                pending = ReferencePublicationRollback.model_validate_json(row["rollback_json"])
                if pending != token:
                    raise ReferenceDataConflictError(
                        "publication completion token does not match pending intent"
                    )
                stage = self._stage_from_row(row)
                if stage is None:
                    raise ReferenceDataIntegrityError(
                        "pending publication does not contain a staged generation"
                    )
                try:
                    receipt = self._validated_completion_receipt(stage, strict=True)
                except ReferenceDataIntegrityError as exc:
                    raise ReferenceDataConflictError(str(exc)) from exc
                if receipt is None:
                    raise ReferenceDataConflictError(
                        "publication completion receipt is missing or invalid"
                    )
                self._commit_stage_in_connection(
                    connection,
                    stage,
                    durable_completed_at=receipt.durable_completed_at,
                )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def finalize_publication(self, rollback: ReferencePublicationRollback) -> None:
        """Expose a staged registry generation only after the cursor is durable."""

        token = ReferencePublicationRollback.model_validate(rollback)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT rollback_json, publication_id, completion_receipt_path, stage_json "
                    "FROM reference_publication_intent WHERE singleton = 1"
                ).fetchone()
                if row is None:
                    return
                pending = ReferencePublicationRollback.model_validate_json(row["rollback_json"])
                if pending != token:
                    raise ReferenceDataConflictError(
                        "publication finalization token does not match pending intent"
                    )
                stage = self._stage_from_row(row)
                if stage is None:
                    raise ReferenceDataIntegrityError(
                        "pending publication does not contain a staged generation"
                    )
                receipt = self._validated_completion_receipt(stage, strict=True)
                if receipt is None:
                    raise ReferenceDataConflictError(
                        "publication completion receipt is missing or invalid"
                    )
                self._commit_stage_in_connection(
                    connection,
                    stage,
                    durable_completed_at=receipt.durable_completed_at,
                )
                connection.execute("DELETE FROM reference_publication_intent WHERE singleton = 1")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def complete_publication(self, rollback: ReferencePublicationRollback) -> None:
        """Compatibility wrapper for registry-only callers."""

        with self.publication_commit_lock():
            self.commit_publication_stage(rollback)
            self.finalize_publication(rollback)

    def _compensate_publication_in_connection(
        self,
        connection: sqlite3.Connection,
        token: ReferencePublicationRollback,
    ) -> None:
        if token.created_generation_id is None:
            return
        current_row = connection.execute(
            "SELECT * FROM reference_current WHERE singleton = 1"
        ).fetchone()
        if current_row is None:
            raise ReferenceDataConflictError(
                "cannot compensate a publication without its current pointer"
            )
        current = self._pointer_from_row(current_row)
        if current.generation_id != token.created_generation_id:
            raise ReferenceDataConflictError("cannot compensate after reference current changed")
        if token.previous_pointer is None:
            connection.execute("DELETE FROM reference_current WHERE singleton = 1")
        else:
            previous = token.previous_pointer
            connection.execute(
                """
                UPDATE reference_current
                SET generation_id = ?, manifest_sha256 = ?, switched_at = ?,
                    previous_generation_id = ?
                WHERE singleton = 1
                """,
                (
                    previous.generation_id,
                    previous.manifest_sha256,
                    _encode_time(previous.switched_at),
                    previous.previous_generation_id,
                ),
            )
        connection.execute(
            "DELETE FROM reference_publication_receipt WHERE generation_id = ?",
            (token.created_generation_id,),
        )
        connection.execute(
            "DELETE FROM reference_generation_member WHERE generation_id = ?",
            (token.created_generation_id,),
        )
        connection.execute(
            "DELETE FROM reference_generation WHERE generation_id = ?",
            (token.created_generation_id,),
        )
        connection.executemany(
            "DELETE FROM reference_record WHERE record_id = ?",
            ((record_id,) for record_id in token.inserted_record_ids),
        )

    def _reject_overlap(
        self,
        connection: sqlite3.Connection,
        candidate: ReferenceRecord,
    ) -> None:
        rows = connection.execute(
            """
            SELECT record_id, dataset_id, business_key, effective_from, effective_to,
                   revision, source, first_available_at, replacement_reason,
                   payload_json, payload_sha256
            FROM reference_record
            WHERE dataset_id = ? AND business_key = ? AND effective_from != ?
            ORDER BY effective_from, revision
            """,
            (candidate.dataset_id, candidate.key, _encode_time(candidate.effective_from)),
        ).fetchall()
        latest: dict[datetime, ReferenceRecord] = {}
        for row in rows:
            record = self._record_from_row(row)
            current = latest.get(record.effective_from)
            if current is None or record.revision > current.revision:
                latest[record.effective_from] = record
        for existing in latest.values():
            if self._periods_overlap(candidate, existing):
                raise ReferenceDataConflictError(
                    f"effective period overlap with lineage {existing.effective_from.isoformat()}"
                )

    @staticmethod
    def _periods_overlap(first: ReferenceRecord, second: ReferenceRecord) -> bool:
        first_before_second_end = (
            second.effective_to is None or first.effective_from < second.effective_to
        )
        second_before_first_end = (
            first.effective_to is None or second.effective_from < first.effective_to
        )
        return first_before_second_end and second_before_first_end

    def records(self, *, dataset_id: str, key: str) -> tuple[ReferenceRecord, ...]:
        with self.publication_commit_lock(exclusive=False), self._connect() as connection:
            self._fail_closed_on_pending_publication(connection)
            rows = connection.execute(
                """
                    SELECT * FROM reference_record
                    WHERE dataset_id = ? AND business_key = ?
                    ORDER BY effective_from, revision
                    """,
                (dataset_id, key),
            ).fetchall()
        return tuple(self._record_from_row(row) for row in rows)

    def latest_lineage_heads(
        self,
        *,
        effective_from: datetime,
    ) -> tuple[ReferenceRecord, ...]:
        """Read every latest lineage head for one effective boundary in one query."""

        with self.publication_commit_lock(exclusive=False), self._connect() as connection:
            self._fail_closed_on_pending_publication(connection)
            rows = connection.execute(
                """
                SELECT record.* FROM reference_record AS record
                JOIN (
                    SELECT dataset_id, business_key, effective_from, MAX(revision) AS revision
                    FROM reference_record
                    WHERE effective_from = ?
                    GROUP BY dataset_id, business_key, effective_from
                ) AS head
                  ON head.dataset_id = record.dataset_id
                 AND head.business_key = record.business_key
                 AND head.effective_from = record.effective_from
                 AND head.revision = record.revision
                ORDER BY record.dataset_id, record.business_key
                """,
                (_encode_time(normalize_aware_utc(effective_from)),),
            ).fetchall()
        return tuple(self._record_from_row(row) for row in rows)

    def publish(self, *, published_at: datetime) -> ReferenceGenerationManifest:
        observed = normalize_aware_utc(published_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._fail_closed_on_pending_write(connection)
                manifest = self._publish_in_connection(connection, observed)
                connection.commit()
                return manifest
            except BaseException:
                connection.rollback()
                raise

    def _publish_in_connection(
        self,
        connection: sqlite3.Connection,
        observed: datetime,
    ) -> ReferenceGenerationManifest:
        current_row = connection.execute(
            "SELECT * FROM reference_current WHERE singleton = 1"
        ).fetchone()
        current = self._pointer_from_row(current_row) if current_row is not None else None
        if current is not None and observed < current.switched_at:
            raise ReferenceDataConflictError("publication time cannot move backwards")

        if current is None:
            rows = connection.execute(
                """
                SELECT * FROM reference_record
                WHERE first_available_at <= ? ORDER BY record_id
                """,
                (_encode_time(observed),),
            ).fetchall()
            current_manifest = None
        else:
            current_manifest = self._generation_in_connection(connection, current.generation_id)
            rows = connection.execute(
                """
                WITH RECURSIVE ancestry(generation_id, previous_generation_id) AS (
                    SELECT generation_id, previous_generation_id
                    FROM reference_generation WHERE generation_id = ?
                    UNION ALL
                    SELECT parent.generation_id, parent.previous_generation_id
                    FROM reference_generation AS parent
                    JOIN ancestry AS child
                      ON parent.generation_id = child.previous_generation_id
                )
                SELECT record.* FROM reference_record AS record
                WHERE record.first_available_at <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM reference_generation_member AS member
                      JOIN ancestry AS generation
                        ON generation.generation_id = member.generation_id
                      WHERE member.record_id = record.record_id
                  )
                ORDER BY record.record_id
                """,
                (current.generation_id, _encode_time(observed)),
            ).fetchall()
        added_records = tuple(self._record_from_row(row) for row in rows)
        added_record_ids = tuple(record.record_id for record in added_records)
        if current is not None and not added_record_ids:
            if current_manifest is None:
                raise ReferenceDataIntegrityError("current generation manifest is unavailable")
            return current_manifest

        counts: Counter[str] = Counter()
        if current_manifest is not None:
            counts.update(dict(current_manifest.dataset_counts))
        counts.update(record.dataset_id for record in added_records)
        row_count = (current_manifest.row_count if current_manifest is not None else 0) + len(
            added_records
        )
        content_sha256 = canonical_sha256(
            {
                "contract": "reference-generation-content/v2",
                "parent_content_sha256": (
                    current_manifest.content_sha256 if current_manifest is not None else None
                ),
                "added_record_ids": added_record_ids,
            }
        )
        manifest = ReferenceGenerationManifest(
            previous_generation_id=current.generation_id if current else None,
            published_at=observed,
            row_count=row_count,
            dataset_counts=dict(counts),
            added_record_ids=added_record_ids,
            content_sha256=content_sha256,
        )
        connection.execute(
            """
            INSERT INTO reference_generation(
                generation_id, previous_generation_id, published_at, row_count,
                dataset_counts_json, content_sha256, manifest_json, manifest_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                manifest.generation_id,
                manifest.previous_generation_id,
                _encode_time(manifest.published_at),
                manifest.row_count,
                _canonical_json(dict(manifest.dataset_counts)),
                manifest.content_sha256,
                manifest.model_dump_json(),
                manifest.manifest_sha256,
            ),
        )
        connection.executemany(
            """
            INSERT INTO reference_generation_member(generation_id, record_id)
            VALUES (?, ?)
            """,
            ((manifest.generation_id, record_id) for record_id in added_record_ids),
        )
        connection.execute(
            """
            INSERT INTO reference_current(
                singleton, generation_id, manifest_sha256, switched_at,
                previous_generation_id
            ) VALUES (1, ?, ?, ?, ?)
            ON CONFLICT(singleton) DO UPDATE SET
                generation_id = excluded.generation_id,
                manifest_sha256 = excluded.manifest_sha256,
                switched_at = excluded.switched_at,
                previous_generation_id = excluded.previous_generation_id
            """,
            (
                manifest.generation_id,
                manifest.manifest_sha256,
                _encode_time(observed),
                current.generation_id if current else None,
            ),
        )
        return manifest

    def _generation_in_connection(
        self,
        connection: sqlite3.Connection,
        generation_id: str,
    ) -> ReferenceGenerationManifest:
        row = connection.execute(
            "SELECT * FROM reference_generation WHERE generation_id = ?",
            (generation_id,),
        ).fetchone()
        if row is None:
            raise ReferenceDataUnavailableError(f"generation {generation_id} does not exist")
        return self._manifest_from_row(row)

    @staticmethod
    def _fail_closed_on_pending_publication(connection: sqlite3.Connection) -> None:
        pending = connection.execute(
            "SELECT 1 FROM reference_publication_intent WHERE singleton = 1"
        ).fetchone()
        if pending is not None:
            raise ReferenceDataUnavailableError(
                "reference publication is pending a durable completion receipt"
            )

    @staticmethod
    def _fail_closed_on_pending_write(connection: sqlite3.Connection) -> None:
        pending = connection.execute(
            "SELECT 1 FROM reference_publication_intent WHERE singleton = 1"
        ).fetchone()
        if pending is not None:
            raise ReferenceDataConflictError(
                "reference publication is pending a durable completion receipt"
            )

    def generation(self, generation_id: str) -> ReferenceGenerationManifest:
        with self.publication_commit_lock(exclusive=False), self._connect() as connection:
            self._fail_closed_on_pending_publication(connection)
            return self._generation_in_connection(connection, generation_id)

    def current_pointer(self) -> ReferenceCurrentPointer:
        with self.publication_commit_lock(exclusive=False), self._connect() as connection:
            self._fail_closed_on_pending_publication(connection)
            row = connection.execute(
                "SELECT * FROM reference_current WHERE singleton = 1"
            ).fetchone()
            if row is None:
                raise ReferenceDataUnavailableError("current reference generation is missing")
            pointer = self._pointer_from_row(row)
            manifest = self._generation_in_connection(connection, pointer.generation_id)
            if pointer.manifest_sha256 != manifest.manifest_sha256:
                raise ReferenceDataIntegrityError("current pointer manifest hash mismatch")
            return pointer

    def current_manifest(self) -> ReferenceGenerationManifest:
        pointer = self.current_pointer()
        return self.generation(pointer.generation_id)

    def rollback(
        self,
        generation_id: str,
        *,
        switched_at: datetime,
    ) -> ReferenceCurrentPointer:
        observed = normalize_aware_utc(switched_at)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._fail_closed_on_pending_write(connection)
                current_row = connection.execute(
                    "SELECT * FROM reference_current WHERE singleton = 1"
                ).fetchone()
                if current_row is None:
                    raise ReferenceDataUnavailableError("current reference generation is missing")
                current = self._pointer_from_row(current_row)
                if observed < current.switched_at:
                    raise ReferenceDataConflictError("rollback time cannot move backwards")
                target = self._generation_in_connection(connection, generation_id)
                if target.generation_id == current.generation_id:
                    connection.rollback()
                    return current
                pointer = ReferenceCurrentPointer(
                    generation_id=target.generation_id,
                    manifest_sha256=target.manifest_sha256,
                    switched_at=observed,
                    previous_generation_id=current.generation_id,
                )
                connection.execute(
                    """
                    UPDATE reference_current
                    SET generation_id = ?, manifest_sha256 = ?, switched_at = ?,
                        previous_generation_id = ?
                    WHERE singleton = 1
                    """,
                    (
                        pointer.generation_id,
                        pointer.manifest_sha256,
                        _encode_time(pointer.switched_at),
                        pointer.previous_generation_id,
                    ),
                )
                connection.commit()
                return pointer
            except BaseException:
                connection.rollback()
                raise

    def as_of(
        self,
        *,
        dataset_id: str,
        key: str,
        event_time: datetime,
        decision_time: datetime,
        generation_id: str | None = None,
    ) -> ReferenceLookup:
        event = normalize_aware_utc(event_time)
        decision = normalize_aware_utc(decision_time)
        with self.publication_commit_lock(exclusive=False), self._connect() as connection:
            self._fail_closed_on_pending_publication(connection)
            selected_generation = generation_id
            if selected_generation is None:
                pointer_row = connection.execute(
                    "SELECT * FROM reference_current WHERE singleton = 1"
                ).fetchone()
                if pointer_row is None:
                    raise ReferenceDataUnavailableError("current reference generation is missing")
                selected_generation = self._pointer_from_row(pointer_row).generation_id
            self._generation_in_connection(connection, selected_generation)
            rows = connection.execute(
                """
                WITH RECURSIVE ancestry(generation_id, previous_generation_id) AS (
                    SELECT generation_id, previous_generation_id
                    FROM reference_generation WHERE generation_id = ?
                    UNION ALL
                    SELECT parent.generation_id, parent.previous_generation_id
                    FROM reference_generation AS parent
                    JOIN ancestry AS child
                      ON parent.generation_id = child.previous_generation_id
                )
                SELECT r.* FROM reference_record AS r
                JOIN reference_generation_member AS m ON m.record_id = r.record_id
                JOIN ancestry AS generation ON generation.generation_id = m.generation_id
                WHERE r.dataset_id = ? AND r.business_key = ?
                ORDER BY r.effective_from, r.revision
                """,
                (selected_generation, dataset_id, key),
            ).fetchall()
        if not rows:
            raise ReferenceDataUnavailableError("reference key is not present in generation")
        records = tuple(self._record_from_row(row) for row in rows)
        visible = tuple(record for record in records if record.first_available_at <= decision)
        if not visible:
            raise ReferenceDataUnavailableError("reference value is not available at decision_time")

        latest: dict[datetime, ReferenceRecord] = {}
        for record in visible:
            existing = latest.get(record.effective_from)
            if existing is None or record.revision > existing.revision:
                latest[record.effective_from] = record
        effective = tuple(
            record
            for record in latest.values()
            if record.effective_from <= event
            and (record.effective_to is None or event < record.effective_to)
        )
        if not effective:
            raise ReferenceDataUnavailableError("reference value is not effective at event_time")
        if len(effective) != 1:
            raise ReferenceDataIntegrityError("overlapping effective reference values")
        return ReferenceLookup(
            record=effective[0],
            generation_id=selected_generation,
            event_time=event,
            decision_time=decision,
        )


class ReadonlyReferenceRegistry(ReferenceRegistry):
    """Read an existing reference authority without initializing or mutating it."""

    def __init__(
        self,
        path: Path | str,
        *,
        busy_timeout_ms: int = 5_000,
        publication_authenticator: ReferencePublicationAuthenticator | None = None,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        candidate = Path(os.path.abspath(Path(path)))
        try:
            observed = candidate.lstat()
        except FileNotFoundError:
            raise ReferenceDataUnavailableError("reference registry is unavailable") from None
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_nlink != 1
        ):
            raise ReferenceDataIntegrityError("reference registry path is unsafe")
        self.path = candidate
        self._publication_lock_path = self.path.with_name(f".{self.path.name}.publication.lock")
        self.busy_timeout_ms = busy_timeout_ms
        self.publication_authenticator = (
            publication_authenticator or ReferencePublicationAuthenticator.from_environment()
        )
        self._database_identity = (observed.st_dev, observed.st_ino)
        try:
            self._validate_integrity()
        except sqlite3.DatabaseError as exc:
            raise ReferenceDataIntegrityError("reference registry is invalid") from exc

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        with _SQLITE_CONNECT_IDENTITY_LOCK, self._connect_locked() as connection:
            yield connection

    @contextmanager
    def _connect_locked(self) -> Iterator[sqlite3.Connection]:
        database_descriptor = -1
        try:
            database_descriptor = os.open(
                self.path,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
        except FileNotFoundError:
            raise ReferenceDataUnavailableError("reference registry is unavailable") from None
        try:
            expected_identity = self._validate_database_file(
                self.path,
                os.fstat(database_descriptor),
            )
            if expected_identity != self._database_identity:
                raise ReferenceDataIntegrityError("reference registry identity changed")
            identities_before_connect = _regular_descriptor_identities()
            try:
                connection = sqlite3.connect(
                    f"{self.path.as_uri()}?mode=ro",
                    uri=True,
                    timeout=self.busy_timeout_ms / 1_000,
                    isolation_level=None,
                )
            except sqlite3.OperationalError as exc:
                raise ReferenceDataUnavailableError("reference registry is unavailable") from exc
            try:
                connection.row_factory = sqlite3.Row
                connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA query_only = ON")
                connection.execute("PRAGMA schema_version").fetchone()
                path_identity = self._validate_database_file(
                    self.path,
                    self.path.lstat(),
                )
                if path_identity != expected_identity:
                    raise ReferenceDataIntegrityError(
                        "reference registry identity changed during connect"
                    )
                self._validate_sqlite_sidecars()
                self._attest_connected_database(
                    identities_before_connect=identities_before_connect,
                    expected_main_identity=expected_identity,
                )
                with connection:
                    yield connection
            finally:
                connection.close()
        finally:
            if database_descriptor >= 0:
                os.close(database_descriptor)
