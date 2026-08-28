"""Dynamic immutable, point-in-time execution constraints for the paper broker."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import secrets
import stat
import threading
from collections.abc import Callable, Mapping
from contextlib import suppress
from datetime import UTC, date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import (
    ConfigDict,
    Field,
    StrictInt,
    ValidationError,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.research_run_spec import InstrumentContext
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.signal_contracts import SignalAction

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
Clock = Callable[[], datetime]
DirectoryEntry = tuple[int, str | None, os.stat_result]
_SHANGHAI = ZoneInfo("Asia/Shanghai")


class PaperExecutionConstraintUnavailableError(RuntimeError):
    """The requested constraint is absent or not valid at the observation time."""


class PaperExecutionConstraintIntegrityError(RuntimeError):
    """The authority path, pointer, or immutable content failed verification."""


class _StrictContractModel(RuntimeContractModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
        strict=True,
    )


class PaperExecutionConstraintSnapshot(_StrictContractModel):
    """One code/date constraint state over an explicit PIT interval."""

    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:BJ|SH|SZ)$")
    trade_date: date
    available_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    suspended: bool
    buy_limit_locked: bool
    sell_limit_locked: bool
    risk_rejected: bool
    instrument_context: InstrumentContext | None = None
    source_snapshot_ids: Mapping[str, Sha256]
    producer_commit: CommitSha
    content_hash: Sha256

    @field_validator("source_snapshot_ids")
    @classmethod
    def freeze_source_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if not value:
            raise ValueError("source_snapshot_ids must not be empty")
        if any(not key.strip() for key in value):
            raise ValueError("source_snapshot_ids keys must be non-empty strings")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_snapshot_ids")
    def serialize_source_snapshot_ids(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.available_at >= self.expires_at:
            raise ValueError("expires_at must be later than available_at")
        if self.available_at.astimezone(_SHANGHAI).date() != self.trade_date:
            raise ValueError("trade_date must match available_at in Asia/Shanghai")
        if self.expires_at.astimezone(_SHANGHAI).date() != self.trade_date:
            raise ValueError("expires_at must remain on trade_date in Asia/Shanghai")
        if self.instrument_context is not None and self.instrument_context.ts_code != self.ts_code:
            raise ValueError("instrument_context ts_code must match constraint ts_code")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("snapshot content_hash does not match canonical content")
        return self


class PaperExecutionConstraintBatch(_StrictContractModel):
    """One immutable ordered generation of execution-constraint intervals."""

    schema_version: Literal[1]
    sequence: StrictInt = Field(ge=0)
    producer_commit: CommitSha
    records: tuple[PaperExecutionConstraintSnapshot, ...] = Field(min_length=1)
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        keys = [(record.trade_date, record.ts_code, record.available_at) for record in self.records]
        if keys != sorted(keys):
            raise ValueError(
                "constraint records must be sorted by trade_date, ts_code, and available_at"
            )
        if any(record.producer_commit != self.producer_commit for record in self.records):
            raise ValueError("record producer_commit does not match batch producer_commit")
        previous_by_key: dict[tuple[date, str], PaperExecutionConstraintSnapshot] = {}
        for record in self.records:
            key = (record.trade_date, record.ts_code)
            previous = previous_by_key.get(key)
            if previous is not None and record.available_at < previous.expires_at:
                raise ValueError("constraint intervals for one code/date must not overlap")
            previous_by_key[key] = record
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("batch content_hash does not match canonical content")
        return self


class PaperExecutionConstraintPointer(_StrictContractModel):
    """Atomic mutable pointer to one immutable constraint generation."""

    schema_version: Literal[1]
    sequence: StrictInt = Field(ge=0)
    batch_hash: Sha256
    file_sha256: Sha256
    published_at: AwareUtcDatetime
    producer_commit: CommitSha
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("pointer content_hash does not match canonical content")
        return self


class PaperExecutionConstraintDecision(_StrictContractModel):
    """Direction-resolved booleans required by BrokerExecutionContext."""

    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:BJ|SH|SZ)$")
    trade_date: date
    available_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    suspended: bool
    limit_locked: bool
    risk_rejected: bool
    instrument_context: InstrumentContext | None
    source_snapshot_ids: Mapping[str, Sha256]
    producer_commit: CommitSha
    constraint_content_hash: Sha256
    batch_content_hash: Sha256
    authority_file_sha256: Sha256

    @field_validator("source_snapshot_ids")
    @classmethod
    def freeze_source_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_snapshot_ids")
    def serialize_source_snapshot_ids(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)


class PaperExecutionConstraintPublisher:
    """Single-writer publisher for atomic current plus retained generations."""

    def __init__(
        self,
        *,
        root: Path,
        producer_commit: str,
        clock: Clock | None = None,
        max_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self.root = _validated_root(root)
        _require_digest(producer_commit, length=40, name="producer_commit")
        self.producer_commit = producer_commit
        self.clock = clock or _system_utc_now
        if not callable(self.clock):
            raise TypeError("clock must be callable")
        self.max_bytes = _require_max_bytes(max_bytes)

    def publish(
        self,
        batch: PaperExecutionConstraintBatch,
    ) -> PaperExecutionConstraintPointer:
        if not isinstance(batch, PaperExecutionConstraintBatch):
            raise TypeError("batch must be PaperExecutionConstraintBatch")
        batch = PaperExecutionConstraintBatch.model_validate(batch)
        if batch.producer_commit != self.producer_commit:
            raise PaperExecutionConstraintIntegrityError(
                "batch producer_commit does not match publisher commit"
            )
        payload = _canonical_json(batch)
        if len(payload) > self.max_bytes:
            raise PaperExecutionConstraintIntegrityError(
                "immutable generation exceeds configured size limit"
            )
        file_sha256 = hashlib.sha256(payload).hexdigest()

        chain = _open_or_create_root(self.root)
        root_fd = chain[-1][0]
        generations_fd = -1
        lock_fd = -1
        try:
            generations_fd = _open_or_create_child_directory(root_fd, "generations")
            lock_fd = _open_publish_lock(root_fd)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            current = _load_current_for_publisher(
                root_fd=root_fd,
                generations_fd=generations_fd,
                max_bytes=self.max_bytes,
            )
            if current is not None:
                current_pointer, current_batch, current_pointer_bytes = current
                if current_pointer.batch_hash == batch.content_hash:
                    if current_batch != batch or current_pointer.file_sha256 != file_sha256:
                        raise PaperExecutionConstraintIntegrityError(
                            "idempotent immutable generation conflicts with current content"
                        )
                    if current_pointer_bytes != _canonical_json(current_pointer):
                        raise PaperExecutionConstraintIntegrityError(
                            "idempotent current pointer bytes conflict"
                        )
                    return current_pointer
                if batch.sequence < current_pointer.sequence:
                    raise PaperExecutionConstraintIntegrityError(
                        "generation sequence rollback rejected"
                    )
                if batch.sequence == current_pointer.sequence:
                    raise PaperExecutionConstraintIntegrityError(
                        "different generation at the current sequence is a rollback"
                    )

            _publish_immutable_generation(
                generations_fd,
                batch_hash=batch.content_hash,
                payload=payload,
                max_bytes=self.max_bytes,
            )
            published_at = _read_clock(self.clock)
            if any(record.available_at > published_at for record in batch.records):
                raise PaperExecutionConstraintIntegrityError(
                    "constraint generation contains future evidence"
                )
            pointer = _build_pointer(
                batch=batch,
                file_sha256=file_sha256,
                published_at=published_at,
            )
            pointer_bytes = _canonical_json(pointer)
            if len(pointer_bytes) > self.max_bytes:
                raise PaperExecutionConstraintIntegrityError(
                    "current pointer exceeds configured size limit"
                )
            if current is not None:
                _reject_publish_rollback(
                    current_pointer=current[0],
                    next_pointer=pointer,
                )
            _replace_current_pointer(root_fd, pointer_bytes)
            return pointer
        finally:
            if lock_fd >= 0:
                with suppress(OSError):
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                with suppress(OSError):
                    os.close(lock_fd)
            with suppress(OSError):
                if generations_fd >= 0:
                    os.close(generations_fd)
            _close_directory_chain(chain)


class PaperExecutionConstraintAuthority:
    """Resolve the dynamically selected immutable generation at a PIT cutoff."""

    def __init__(
        self,
        *,
        root: Path,
        expected_producer_commit: str,
        max_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self.root = _validated_root(root)
        _require_digest(
            expected_producer_commit,
            length=40,
            name="expected_producer_commit",
        )
        self.expected_producer_commit = expected_producer_commit
        self.max_bytes = _require_max_bytes(max_bytes)
        self._watermark_lock = threading.Lock()
        self._last_observation: tuple[datetime, int, str] | None = None

    def load(self, *, observed_at: datetime) -> PaperExecutionConstraintBatch:
        batch, _pointer = self._load_generation(observed_at=observed_at)
        return batch

    def resolve(
        self,
        *,
        ts_code: str,
        trade_date: date,
        observed_at: datetime,
        action: SignalAction,
    ) -> PaperExecutionConstraintDecision:
        observed = _normalize_observed_at(observed_at)
        if action is SignalAction.B_INTENT:
            direction = "buy"
        elif action in {SignalAction.REDUCE, SignalAction.S_INTENT}:
            direction = "sell"
        else:
            raise PaperExecutionConstraintUnavailableError(
                f"{action.value} is not an execution action"
            )

        batch, pointer = self._load_generation(observed_at=observed)
        matching = tuple(
            record
            for record in batch.records
            if record.ts_code == ts_code and record.trade_date == trade_date
        )
        record = next(
            (
                candidate
                for candidate in matching
                if candidate.available_at <= observed < candidate.expires_at
            ),
            None,
        )
        if record is None:
            if matching and observed < matching[0].available_at:
                raise PaperExecutionConstraintUnavailableError("constraint is not yet available")
            if matching and observed >= matching[-1].expires_at:
                raise PaperExecutionConstraintUnavailableError("constraint has expired")
            raise PaperExecutionConstraintUnavailableError(
                f"constraint interval not found for {ts_code} on {trade_date.isoformat()}"
            )

        return PaperExecutionConstraintDecision(
            ts_code=record.ts_code,
            trade_date=record.trade_date,
            available_at=record.available_at,
            expires_at=record.expires_at,
            suspended=record.suspended,
            limit_locked=(
                record.buy_limit_locked if direction == "buy" else record.sell_limit_locked
            ),
            risk_rejected=record.risk_rejected,
            instrument_context=record.instrument_context,
            source_snapshot_ids=record.source_snapshot_ids,
            producer_commit=record.producer_commit,
            constraint_content_hash=record.content_hash,
            batch_content_hash=batch.content_hash,
            authority_file_sha256=pointer.file_sha256,
        )

    def _load_generation(
        self,
        *,
        observed_at: datetime,
    ) -> tuple[PaperExecutionConstraintBatch, PaperExecutionConstraintPointer]:
        observed = _normalize_observed_at(observed_at)
        try:
            chain = _open_existing_directory_chain(self.root)
        except FileNotFoundError as exc:
            raise PaperExecutionConstraintUnavailableError(
                "constraint authority is unavailable"
            ) from exc
        except OSError as exc:
            raise PaperExecutionConstraintIntegrityError(
                "constraint authority root is unsafe or contains a symlink"
            ) from exc
        root_fd = chain[-1][0]
        generations_fd = -1
        try:
            pointer_bytes = _read_regular_file_at(
                root_fd,
                "current.json",
                max_bytes=self.max_bytes,
                label="current pointer",
                missing_unavailable=True,
            )
            assert pointer_bytes is not None
            pointer = _parse_pointer(pointer_bytes)
            if pointer.producer_commit != self.expected_producer_commit:
                raise PaperExecutionConstraintIntegrityError(
                    "current pointer producer_commit does not match expected commit"
                )
            if pointer.published_at > observed:
                raise PaperExecutionConstraintUnavailableError(
                    "constraint authority is not yet available at observed_at"
                )
            try:
                generations_fd = _open_existing_child_directory(root_fd, "generations")
            except FileNotFoundError as exc:
                raise PaperExecutionConstraintIntegrityError(
                    "current pointer has a generation gap"
                ) from exc
            chain.append(_directory_entry(root_fd, generations_fd, "generations"))
            try:
                batch_bytes = _read_regular_file_at(
                    generations_fd,
                    f"{pointer.batch_hash}.json",
                    max_bytes=self.max_bytes,
                    label="immutable generation",
                    missing_unavailable=False,
                )
            except FileNotFoundError as exc:
                raise PaperExecutionConstraintIntegrityError(
                    "current pointer has a generation gap"
                ) from exc
            assert batch_bytes is not None
            if hashlib.sha256(batch_bytes).hexdigest() != pointer.file_sha256:
                raise PaperExecutionConstraintIntegrityError(
                    "immutable generation file sha256 does not match current pointer"
                )
            batch = _parse_batch(batch_bytes)
            _validate_pointer_batch_binding(pointer, batch)

            current_after = _read_regular_file_at(
                root_fd,
                "current.json",
                max_bytes=self.max_bytes,
                label="current pointer",
                missing_unavailable=True,
            )
            if current_after != pointer_bytes:
                raise PaperExecutionConstraintIntegrityError(
                    "current pointer changed while reading generation"
                )
            _verify_directory_chain(chain)
            self._accept_monotonic(pointer)
            return batch, pointer
        except PaperExecutionConstraintUnavailableError:
            raise
        except PaperExecutionConstraintIntegrityError:
            raise
        except FileNotFoundError as exc:
            raise PaperExecutionConstraintUnavailableError(
                "constraint authority is unavailable"
            ) from exc
        except OSError as exc:
            raise PaperExecutionConstraintIntegrityError(
                "constraint authority path is unsafe or contains a symlink"
            ) from exc
        finally:
            _close_directory_chain(chain)

    def _accept_monotonic(self, pointer: PaperExecutionConstraintPointer) -> None:
        observation = (pointer.published_at, pointer.sequence, pointer.batch_hash)
        with self._watermark_lock:
            previous = self._last_observation
            if previous is not None:
                previous_published_at, previous_sequence, previous_batch_hash = previous
                if pointer.published_at < previous_published_at:
                    raise PaperExecutionConstraintIntegrityError(
                        "constraint authority publication rollback detected"
                    )
                if pointer.sequence < previous_sequence:
                    raise PaperExecutionConstraintIntegrityError(
                        "constraint authority sequence rollback detected"
                    )
                if (
                    pointer.sequence == previous_sequence
                    and pointer.batch_hash != previous_batch_hash
                ):
                    raise PaperExecutionConstraintIntegrityError(
                        "constraint authority generation rollback detected"
                    )
            self._last_observation = observation


def _build_pointer(
    *,
    batch: PaperExecutionConstraintBatch,
    file_sha256: str,
    published_at: datetime,
) -> PaperExecutionConstraintPointer:
    values: dict[str, object] = {
        "schema_version": 1,
        "sequence": batch.sequence,
        "batch_hash": batch.content_hash,
        "file_sha256": file_sha256,
        "published_at": published_at,
        "producer_commit": batch.producer_commit,
    }
    values["content_hash"] = canonical_sha256(values)
    return PaperExecutionConstraintPointer.model_validate(values)


def _parse_pointer(payload: bytes) -> PaperExecutionConstraintPointer:
    try:
        return PaperExecutionConstraintPointer.model_validate_json(payload)
    except (ValidationError, ValueError) as exc:
        raise PaperExecutionConstraintIntegrityError("current pointer is invalid") from exc


def _parse_batch(payload: bytes) -> PaperExecutionConstraintBatch:
    try:
        return PaperExecutionConstraintBatch.model_validate_json(payload)
    except (ValidationError, ValueError) as exc:
        raise PaperExecutionConstraintIntegrityError(
            "immutable generation is not a valid constraint batch"
        ) from exc


def _canonical_json(model: RuntimeContractModel) -> bytes:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _validate_pointer_batch_binding(
    pointer: PaperExecutionConstraintPointer,
    batch: PaperExecutionConstraintBatch,
) -> None:
    if pointer.batch_hash != batch.content_hash:
        raise PaperExecutionConstraintIntegrityError(
            "current pointer batch_hash does not match immutable generation"
        )
    if pointer.sequence != batch.sequence:
        raise PaperExecutionConstraintIntegrityError(
            "current pointer sequence does not match immutable generation"
        )
    if pointer.producer_commit != batch.producer_commit:
        raise PaperExecutionConstraintIntegrityError(
            "current pointer producer_commit does not match immutable generation"
        )


def _reject_publish_rollback(
    *,
    current_pointer: PaperExecutionConstraintPointer,
    next_pointer: PaperExecutionConstraintPointer,
) -> None:
    if next_pointer.sequence < current_pointer.sequence:
        raise PaperExecutionConstraintIntegrityError("generation sequence rollback rejected")
    if next_pointer.sequence == current_pointer.sequence:
        raise PaperExecutionConstraintIntegrityError(
            "different generation at the current sequence is a rollback"
        )
    if next_pointer.published_at < current_pointer.published_at:
        raise PaperExecutionConstraintIntegrityError("generation publication rollback rejected")


def _load_current_for_publisher(
    *,
    root_fd: int,
    generations_fd: int,
    max_bytes: int,
) -> (
    tuple[
        PaperExecutionConstraintPointer,
        PaperExecutionConstraintBatch,
        bytes,
    ]
    | None
):
    pointer_bytes = _read_regular_file_at(
        root_fd,
        "current.json",
        max_bytes=max_bytes,
        label="current pointer",
        missing_unavailable=False,
        optional=True,
    )
    if pointer_bytes is None:
        return None
    pointer = _parse_pointer(pointer_bytes)
    try:
        batch_bytes = _read_regular_file_at(
            generations_fd,
            f"{pointer.batch_hash}.json",
            max_bytes=max_bytes,
            label="immutable generation",
            missing_unavailable=False,
        )
    except FileNotFoundError as exc:
        raise PaperExecutionConstraintIntegrityError(
            "current pointer has a generation gap"
        ) from exc
    assert batch_bytes is not None
    if hashlib.sha256(batch_bytes).hexdigest() != pointer.file_sha256:
        raise PaperExecutionConstraintIntegrityError(
            "immutable generation file sha256 conflicts with current pointer"
        )
    batch = _parse_batch(batch_bytes)
    _validate_pointer_batch_binding(pointer, batch)
    return pointer, batch, pointer_bytes


def _publish_immutable_generation(
    directory_fd: int,
    *,
    batch_hash: str,
    payload: bytes,
    max_bytes: int,
) -> None:
    name = f"{batch_hash}.json"
    existing = _read_regular_file_at(
        directory_fd,
        name,
        max_bytes=max_bytes,
        label="immutable generation",
        missing_unavailable=False,
        optional=True,
    )
    if existing is not None:
        if existing != payload:
            raise PaperExecutionConstraintIntegrityError(
                "immutable generation already exists with conflicting content"
            )
        return

    temporary = f".{batch_hash}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=directory_fd,
        )
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(
                temporary,
                name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            existing = _read_regular_file_at(
                directory_fd,
                name,
                max_bytes=max_bytes,
                label="immutable generation",
                missing_unavailable=False,
            )
            if existing != payload:
                raise PaperExecutionConstraintIntegrityError(
                    "immutable generation publication conflicted with existing content"
                ) from exc
        os.fsync(directory_fd)
    finally:
        with suppress(OSError):
            if descriptor >= 0:
                os.close(descriptor)
        with suppress(OSError):
            os.unlink(temporary, dir_fd=directory_fd)


def _replace_current_pointer(root_fd: int, payload: bytes) -> None:
    temporary = f".current.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o600,
            dir_fd=root_fd,
        )
        _write_all(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.rename(
            temporary,
            "current.json",
            src_dir_fd=root_fd,
            dst_dir_fd=root_fd,
        )
        os.fsync(root_fd)
    finally:
        with suppress(OSError):
            if descriptor >= 0:
                os.close(descriptor)
        with suppress(OSError):
            os.unlink(temporary, dir_fd=root_fd)


def _open_or_create_root(root: Path) -> list[DirectoryEntry]:
    parent_chain = _open_existing_directory_chain(root.parent)
    parent_fd = parent_chain[-1][0]
    try:
        root_fd = _open_existing_child_directory(parent_fd, root.name)
    except FileNotFoundError:
        os.mkdir(root.name, mode=0o700, dir_fd=parent_fd)
        os.fsync(parent_fd)
        root_fd = _open_existing_child_directory(parent_fd, root.name)
    parent_chain.append(_directory_entry(parent_fd, root_fd, root.name))
    return parent_chain


def _open_existing_directory_chain(path: Path) -> list[DirectoryEntry]:
    chain: list[DirectoryEntry] = []
    try:
        root_fd = os.open("/", _directory_open_flags())
        root_stat = os.fstat(root_fd)
        if not stat.S_ISDIR(root_stat.st_mode):
            raise PaperExecutionConstraintIntegrityError("filesystem root is unsafe")
        chain.append((root_fd, None, root_stat))
        for component in path.parts[1:]:
            parent_fd = chain[-1][0]
            child_fd = os.open(component, _directory_open_flags(), dir_fd=parent_fd)
            chain.append(_directory_entry(parent_fd, child_fd, component))
        return chain
    except Exception:
        _close_directory_chain(chain)
        raise


def _open_existing_child_directory(parent_fd: int, name: str) -> int:
    return os.open(name, _directory_open_flags(), dir_fd=parent_fd)


def _open_or_create_child_directory(parent_fd: int, name: str) -> int:
    try:
        return _open_existing_child_directory(parent_fd, name)
    except FileNotFoundError:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
        os.fsync(parent_fd)
        return _open_existing_child_directory(parent_fd, name)


def _directory_entry(parent_fd: int, child_fd: int, name: str) -> DirectoryEntry:
    child = os.fstat(child_fd)
    at_path = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if not _same_directory(child, at_path):
        os.close(child_fd)
        raise PaperExecutionConstraintIntegrityError("authority directory identity is unsafe")
    return child_fd, name, child


def _open_publish_lock(root_fd: int) -> int:
    descriptor = os.open(
        ".publish.lock",
        os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        0o600,
        dir_fd=root_fd,
    )
    opened = os.fstat(descriptor)
    at_path = os.stat(".publish.lock", dir_fd=root_fd, follow_symlinks=False)
    if not _same_regular_file(opened, at_path):
        os.close(descriptor)
        raise PaperExecutionConstraintIntegrityError("publisher lock identity is unsafe")
    return descriptor


def _read_regular_file_at(
    directory_fd: int,
    name: str,
    *,
    max_bytes: int,
    label: str,
    missing_unavailable: bool,
    optional: bool = False,
) -> bytes | None:
    descriptor = -1
    try:
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_fd,
        )
        before = os.fstat(descriptor)
        at_path_before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not _same_regular_file(before, at_path_before):
            raise PaperExecutionConstraintIntegrityError(f"{label} identity is unsafe")
        if before.st_size > max_bytes:
            raise PaperExecutionConstraintIntegrityError(f"{label} exceeds configured size limit")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > max_bytes:
            raise PaperExecutionConstraintIntegrityError(f"{label} exceeds configured size limit")
        after = os.fstat(descriptor)
        at_path_after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if not _same_observation(before, after) or not _same_regular_file(
            after,
            at_path_after,
        ):
            raise PaperExecutionConstraintIntegrityError(f"{label} changed while being read")
        return payload
    except FileNotFoundError as exc:
        if optional:
            return None
        if missing_unavailable:
            raise PaperExecutionConstraintUnavailableError(f"{label} is unavailable") from exc
        raise
    except PaperExecutionConstraintUnavailableError:
        raise
    except PaperExecutionConstraintIntegrityError:
        raise
    except OSError as exc:
        raise PaperExecutionConstraintIntegrityError(
            f"{label} path is unsafe or contains a symlink"
        ) from exc
    finally:
        with suppress(OSError):
            if descriptor >= 0:
                os.close(descriptor)


def _verify_directory_chain(chain: list[DirectoryEntry]) -> None:
    for index, (directory_fd, name, initial) in enumerate(chain):
        current = os.fstat(directory_fd)
        if not _same_observation(initial, current) or not stat.S_ISDIR(current.st_mode):
            raise PaperExecutionConstraintIntegrityError(
                "authority directory changed while being read"
            )
        if index == 0:
            continue
        parent_fd = chain[index - 1][0]
        assert name is not None
        at_path = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if not _same_directory(current, at_path):
            raise PaperExecutionConstraintIntegrityError(
                "authority directory changed while being read"
            )


def _close_directory_chain(chain: list[DirectoryEntry]) -> None:
    seen: set[int] = set()
    for descriptor, _name, _initial in reversed(chain):
        if descriptor in seen:
            continue
        seen.add(descriptor)
        with suppress(OSError):
            os.close(descriptor)


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("short write while publishing execution constraints")
        offset += written


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )


def _validated_root(root: Path) -> Path:
    value = Path(root)
    if not value.is_absolute() or len(value.parts) < 2:
        raise ValueError("authority root must be an absolute non-root path")
    if any(component in {"", ".", ".."} for component in value.parts[1:]):
        raise ValueError("authority root contains an unsafe component")
    return value


def _normalize_observed_at(observed_at: datetime) -> datetime:
    if not isinstance(observed_at, datetime):
        raise PaperExecutionConstraintUnavailableError(
            "observed_at must be a timezone-aware datetime"
        )
    try:
        return normalize_aware_utc(observed_at)
    except ValueError as exc:
        raise PaperExecutionConstraintUnavailableError(
            "observed_at must be timezone-aware"
        ) from exc


def _read_clock(clock: Clock) -> datetime:
    try:
        return normalize_aware_utc(clock())
    except (TypeError, ValueError) as exc:
        raise PaperExecutionConstraintIntegrityError(
            "publisher clock must return a timezone-aware datetime"
        ) from exc


def _system_utc_now() -> datetime:
    return datetime.now(UTC)


def _require_digest(value: str, *, length: int, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != length
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{name} must be a lowercase hexadecimal digest")


def _require_max_bytes(value: int) -> int:
    if type(value) is not int or value < 1:
        raise ValueError("max_bytes must be a positive integer")
    return value


def _same_directory(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        stat.S_ISDIR(first.st_mode)
        and stat.S_ISDIR(second.st_mode)
        and _same_observation(first, second)
    )


def _same_regular_file(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        stat.S_ISREG(first.st_mode)
        and stat.S_ISREG(second.st_mode)
        and first.st_nlink == 1
        and second.st_nlink == 1
        and _same_observation(first, second)
    )


def _same_observation(first: os.stat_result, second: os.stat_result) -> bool:
    return (
        first.st_dev,
        first.st_ino,
        first.st_mode,
        first.st_size,
        first.st_mtime_ns,
        first.st_ctime_ns,
    ) == (
        second.st_dev,
        second.st_ino,
        second.st_mode,
        second.st_size,
        second.st_mtime_ns,
        second.st_ctime_ns,
    )


__all__ = [
    "PaperExecutionConstraintAuthority",
    "PaperExecutionConstraintBatch",
    "PaperExecutionConstraintDecision",
    "PaperExecutionConstraintIntegrityError",
    "PaperExecutionConstraintPointer",
    "PaperExecutionConstraintPublisher",
    "PaperExecutionConstraintSnapshot",
    "PaperExecutionConstraintUnavailableError",
]
