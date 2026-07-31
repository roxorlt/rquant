"""Point-in-time immutable strategy candidate snapshots."""

from __future__ import annotations

import fcntl
import json
import os
import re
import stat
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from threading import RLock
from types import MappingProxyType
from typing import Annotated, Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic import (
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_GENERATIONS = 4_096
_MAX_AUTHORITY_BYTES = 16 * 1024 * 1024
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_ASIA_SHANGHAI = ZoneInfo("Asia/Shanghai")


class StrategyCandidateSnapshotIntegrityError(RuntimeError):
    """Raised when the immutable candidate snapshot authority is unsafe."""


class StrategyCandidatePriceBasis(StrEnum):
    RAW = "raw"
    QFQ_PIT = "qfq_pit"


def asia_shanghai_trade_date(value: datetime) -> date:
    return normalize_aware_utc(value).astimezone(_ASIA_SHANGHAI).date()


def strategy_candidate_decision_trade_date(
    value: datetime,
    *,
    legacy_utc_date_semantics: bool,
) -> date:
    normalized = normalize_aware_utc(value)
    if legacy_utc_date_semantics:
        return normalized.date()
    return asia_shanghai_trade_date(normalized)


def _freeze_json(value: JsonValue) -> JsonValue:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze_json(item) for key, item in sorted(value.items())})  # type: ignore[return-value]
    if isinstance(value, list):
        return tuple(_freeze_json(item) for item in value)  # type: ignore[return-value]
    return value


def _thaw_json(value: object) -> JsonValue:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("JSON object keys must be strings")
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_thaw_json(item) for item in value]
    return value  # type: ignore[return-value]


def canonicalize_candidate_static_features(value: object) -> Mapping[str, JsonValue]:
    thawed = _thaw_json(value)
    if not isinstance(thawed, dict):
        raise ValueError("static_features must be a JSON object")
    if any(not isinstance(key, str) or not key for key in thawed):
        raise ValueError("static_features keys must be non-empty strings")
    detached = json.loads(
        json.dumps(thawed, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    )
    return MappingProxyType({key: _freeze_json(item) for key, item in sorted(detached.items())})


def serialize_candidate_static_features(
    value: Mapping[str, JsonValue],
) -> dict[str, JsonValue]:
    return {key: _thaw_json(item) for key, item in value.items()}


def thaw_candidate_static_features(value: object) -> JsonValue:
    return _thaw_json(value)


def candidate_occurrence_id(
    *,
    strategy_id: str,
    strategy_version: str,
    candidate_id: str,
    variant: str,
    effective_trade_date: date,
) -> str:
    return canonical_sha256(
        {
            "strategy_id": strategy_id,
            "strategy_version": strategy_version,
            "candidate_id": candidate_id,
            "variant": variant,
            "effective_trade_date": effective_trade_date,
        }
    )


def _snapshot_content_identity(
    *,
    schema_version: Literal[1, 2],
    sequence: int,
    trade_date: date,
    captured_at: datetime,
    producer_commit: str,
    rows: Sequence[StrategyCandidateRecord],
) -> dict[str, object]:
    if schema_version == 1:
        canonical_rows = tuple(
            sorted(
                rows,
                key=lambda row: (row.strategy_id, row.strategy_version, row.candidate_id),
            )
        )
        row_payloads: list[dict[str, object]] = []
        for row in canonical_rows:
            payload = row.model_dump(mode="python")
            payload.pop("effective_trade_date")
            row_payloads.append(payload)
        return {
            "sequence": sequence,
            "trade_date": trade_date,
            "captured_at": normalize_aware_utc(captured_at),
            "producer_commit": producer_commit,
            "rows": tuple(row_payloads),
        }
    canonical_rows = tuple(sorted(rows, key=lambda row: row.identity))
    return {
        "schema_version": schema_version,
        "sequence": sequence,
        "trade_date": trade_date,
        "captured_at": normalize_aware_utc(captured_at),
        "producer_commit": producer_commit,
        "rows": canonical_rows,
    }


def strategy_candidate_snapshot_content_sha256(
    *,
    schema_version: Literal[1, 2],
    sequence: int,
    trade_date: date,
    captured_at: datetime,
    producer_commit: str,
    rows: Sequence[StrategyCandidateRecord],
) -> str:
    return canonical_sha256(
        _snapshot_content_identity(
            schema_version=schema_version,
            sequence=sequence,
            trade_date=trade_date,
            captured_at=captured_at,
            producer_commit=producer_commit,
            rows=rows,
        )
    )


class StrategyCandidateRecord(RuntimeContractModel):
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    candidate_id: str = Field(min_length=1)
    variant: str = Field(min_length=1)
    decision_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    effective_trade_date: date
    reference_trade_date: date
    price_basis: StrategyCandidatePriceBasis
    static_features: Mapping[str, JsonValue]
    reference_snapshot_ids: Mapping[str, Sha256]
    legacy_utc_date_semantics: bool = Field(default=False, exclude=True, repr=False)

    @field_validator("static_features", mode="before")
    @classmethod
    def thaw_static_features_for_validation(cls, value: object) -> JsonValue:
        return thaw_candidate_static_features(value)

    @field_validator("static_features")
    @classmethod
    def freeze_static_features(
        cls,
        value: object,
    ) -> Mapping[str, JsonValue]:
        return canonicalize_candidate_static_features(value)

    @field_validator("reference_snapshot_ids")
    @classmethod
    def freeze_reference_snapshot_ids(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not isinstance(key, str) or not key for key in value):
            raise ValueError("reference_snapshot_ids keys must be non-empty strings")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("static_features")
    def serialize_static_features(
        self,
        value: Mapping[str, JsonValue],
    ) -> dict[str, JsonValue]:
        return serialize_candidate_static_features(value)

    @field_serializer("reference_snapshot_ids")
    def serialize_reference_snapshot_ids(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_point_in_time(self) -> StrategyCandidateRecord:
        if self.available_at < self.decision_at:
            raise ValueError("available_at must be at or after decision_at")
        decision_trade_date = strategy_candidate_decision_trade_date(
            self.decision_at,
            legacy_utc_date_semantics=self.legacy_utc_date_semantics,
        )
        if self.effective_trade_date < decision_trade_date:
            raise ValueError("effective_trade_date cannot precede decision_at date")
        if self.reference_trade_date > decision_trade_date:
            raise ValueError("reference_trade_date cannot be a future reference")
        return self

    @property
    def identity(self) -> tuple[str, str, str, date]:
        return (
            self.strategy_id,
            self.strategy_version,
            self.candidate_id,
            self.effective_trade_date,
        )

    @property
    def occurrence_id(self) -> str:
        return candidate_occurrence_id(
            strategy_id=self.strategy_id,
            strategy_version=self.strategy_version,
            candidate_id=self.candidate_id,
            variant=self.variant,
            effective_trade_date=self.effective_trade_date,
        )


class StrategyCandidateSnapshot(RuntimeContractModel):
    schema_version: Literal[1, 2]
    sequence: int = Field(ge=0)
    trade_date: date
    captured_at: AwareUtcDatetime
    producer_commit: CommitSha
    rows: tuple[StrategyCandidateRecord, ...]
    content_sha256: Sha256

    @field_validator("rows")
    @classmethod
    def canonicalize_rows(
        cls,
        value: tuple[StrategyCandidateRecord, ...],
    ) -> tuple[StrategyCandidateRecord, ...]:
        return tuple(sorted(value, key=lambda row: row.identity))

    @model_validator(mode="after")
    def validate_snapshot(self) -> StrategyCandidateSnapshot:
        if self.schema_version == 1 and any(not row.legacy_utc_date_semantics for row in self.rows):
            raise ValueError("schema v1 rows require legacy UTC date semantics")
        if self.schema_version == 2 and any(row.legacy_utc_date_semantics for row in self.rows):
            raise ValueError("schema v2 rows reject legacy UTC date semantics")
        if self.schema_version == 1:
            for row in self.rows:
                decision_trade_date = strategy_candidate_decision_trade_date(
                    row.decision_at,
                    legacy_utc_date_semantics=True,
                )
                if not (decision_trade_date == row.effective_trade_date == self.trade_date):
                    raise ValueError(
                        "schema v1 decision date must equal effective and snapshot trade date"
                    )
        identities = [row.identity for row in self.rows]
        if len(identities) != len(set(identities)):
            raise ValueError("snapshot contains a duplicate candidate")
        for row in self.rows:
            if row.effective_trade_date != self.trade_date:
                raise ValueError("candidate effective_trade_date must match snapshot trade_date")
            if row.reference_trade_date > self.trade_date:
                raise ValueError("candidate contains a future trade-date reference")
            if row.available_at > self.captured_at:
                raise ValueError("candidate available_at cannot exceed captured_at")
        expected = strategy_candidate_snapshot_content_sha256(
            schema_version=self.schema_version,
            sequence=self.sequence,
            trade_date=self.trade_date,
            captured_at=self.captured_at,
            producer_commit=self.producer_commit,
            rows=self.rows,
        )
        if self.content_sha256 != expected:
            raise ValueError("content_sha256 does not bind canonical snapshot content")
        return self

    @classmethod
    def build(
        cls,
        *,
        sequence: int,
        trade_date: date,
        captured_at: datetime,
        producer_commit: str,
        rows: Sequence[StrategyCandidateRecord],
    ) -> StrategyCandidateSnapshot:
        normalized_captured_at = normalize_aware_utc(captured_at)
        canonical_rows = tuple(sorted(rows, key=lambda row: row.identity))
        identity = _snapshot_content_identity(
            schema_version=2,
            sequence=sequence,
            trade_date=trade_date,
            captured_at=normalized_captured_at,
            producer_commit=producer_commit,
            rows=canonical_rows,
        )
        return cls(
            **identity,
            content_sha256=canonical_sha256(identity),
        )


class StrategyCandidateSnapshotPointer(RuntimeContractModel):
    generation_sha256: Sha256
    sequence: int = Field(ge=0)
    trade_date: date
    captured_at: AwareUtcDatetime
    producer_commit: CommitSha

    @classmethod
    def from_snapshot(
        cls,
        snapshot: StrategyCandidateSnapshot,
    ) -> StrategyCandidateSnapshotPointer:
        return cls(
            generation_sha256=snapshot.content_sha256,
            sequence=snapshot.sequence,
            trade_date=snapshot.trade_date,
            captured_at=snapshot.captured_at,
            producer_commit=snapshot.producer_commit,
        )


class StrategyCandidateSnapshotSpool:
    """Publish and resolve immutable point-in-time candidate generations."""

    def __init__(self, root: Path) -> None:
        candidate = Path(root)
        if not candidate.is_absolute():
            raise ValueError("strategy candidate snapshot root must be absolute")
        normalized = Path(os.path.abspath(candidate))
        if candidate != normalized:
            raise ValueError("strategy candidate snapshot root must be normalized")
        probe = candidate
        while True:
            try:
                probe.lstat()
                break
            except FileNotFoundError:
                if probe == Path(probe.anchor):
                    raise
                probe = probe.parent
        descriptor = self._open_directory(probe, private_final=False)
        os.close(descriptor)
        self.root = candidate
        self.generations_root = self.root / "generations"
        self.current_path = self.root / "current.json"
        self._lock_path = self.root / ".publish.lock"
        self._thread_lock = RLock()
        self._generation_states: dict[str, tuple[int, ...]] = {}
        self._generation_snapshots: dict[str, StrategyCandidateSnapshot] = {}

    def publish(self, snapshot: StrategyCandidateSnapshot) -> StrategyCandidateSnapshot:
        if not isinstance(snapshot, StrategyCandidateSnapshot):
            raise TypeError("snapshot must be a StrategyCandidateSnapshot")
        if snapshot.schema_version != 2:
            raise StrategyCandidateSnapshotIntegrityError(
                "only schema v2 snapshots may be published"
            )
        self._initialize_for_publish()
        with self._locked(exclusive=True) as (root_fd, generations_fd):
            self._cleanup_stale_temporaries(root_fd)
            generations = self._read_all_generations(generations_fd)
            try:
                self._validate_current_pointer(root_fd, generations)
            except StrategyCandidateSnapshotIntegrityError:
                if self._finish_interrupted_publish(root_fd, generations, snapshot):
                    return snapshot
                raise
            existing = generations.get(snapshot.sequence)
            if existing is not None:
                if existing != snapshot:
                    raise StrategyCandidateSnapshotIntegrityError(
                        "immutable sequence already contains different content"
                    )
                return existing
            if len(generations) >= _MAX_GENERATIONS:
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy candidate generation count exceeds limit"
                )
            expected_sequence = 0 if not generations else max(generations) + 1
            if snapshot.sequence != expected_sequence:
                raise StrategyCandidateSnapshotIntegrityError(
                    f"next sequence must be {expected_sequence}, got {snapshot.sequence}"
                )
            if generations and snapshot.captured_at < generations[max(generations)].captured_at:
                raise StrategyCandidateSnapshotIntegrityError(
                    "captured_at cannot move backwards across sequences"
                )
            generation_name = self._generation_name(snapshot.content_sha256)
            if self._entry_exists(generations_fd, generation_name):
                raise StrategyCandidateSnapshotIntegrityError(
                    "generation hash already exists with conflicting sequence authority"
                )
            self._atomic_create_generation(
                root_fd,
                generations_fd,
                generation_name,
                self._model_bytes(snapshot),
            )
            self._atomic_replace_pointer(
                root_fd,
                self._model_bytes(StrategyCandidateSnapshotPointer.from_snapshot(snapshot)),
            )
            return snapshot

    def read_as_of(self, as_of: datetime) -> StrategyCandidateSnapshot | None:
        normalized_as_of = normalize_aware_utc(as_of)
        with self._locked(exclusive=False) as (root_fd, generations_fd):
            generations = self._read_all_generations(generations_fd)
            self._validate_current_pointer(root_fd, generations)
            visible = [
                snapshot
                for snapshot in generations.values()
                if snapshot.captured_at <= normalized_as_of
                and all(row.available_at <= normalized_as_of for row in snapshot.rows)
            ]
            return None if not visible else max(visible, key=lambda item: item.sequence)

    @staticmethod
    def _same_file(left: os.stat_result, right: os.stat_result) -> bool:
        return (
            left.st_dev,
            left.st_ino,
            left.st_mode,
            left.st_uid,
            left.st_nlink,
        ) == (
            right.st_dev,
            right.st_ino,
            right.st_mode,
            right.st_uid,
            right.st_nlink,
        )

    @staticmethod
    def _validate_private_directory(observed: os.stat_result, *, label: str) -> None:
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) != _PRIVATE_DIRECTORY_MODE
        ):
            raise StrategyCandidateSnapshotIntegrityError(f"unsafe {label}")

    @classmethod
    def _open_directory(cls, path: Path, *, private_final: bool) -> int:
        descriptor = -1
        child = -1
        try:
            descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
            for component in path.parts[1:]:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                if stat.S_ISLNK(before.st_mode):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot path contains a symlink"
                    )
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
                opened = os.fstat(child)
                active = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                if not cls._same_file(before, opened) or not cls._same_file(opened, active):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot path identity changed"
                    )
                os.close(descriptor)
                descriptor = child
                child = -1
            if private_final:
                cls._validate_private_directory(
                    os.fstat(descriptor), label="strategy candidate snapshot directory"
                )
            return descriptor
        except OSError as exc:
            if child >= 0:
                os.close(child)
            if descriptor >= 0:
                os.close(descriptor)
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate snapshot directory is missing or contains a symlink"
            ) from exc
        except BaseException:
            if child >= 0:
                os.close(child)
            if descriptor >= 0:
                os.close(descriptor)
            raise

    @classmethod
    def _open_or_create_private_directory(cls, path: Path) -> int:
        descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
        child = -1
        try:
            for index, component in enumerate(path.parts[1:], start=1):
                created = False
                try:
                    before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                except FileNotFoundError:
                    with suppress(FileExistsError):
                        os.mkdir(component, _PRIVATE_DIRECTORY_MODE, dir_fd=descriptor)
                    before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                    created = True
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
                opened = os.fstat(child)
                active = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
                if not cls._same_file(before, opened) or not cls._same_file(opened, active):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot directory identity changed"
                    )
                if created or index == len(path.parts) - 1:
                    cls._validate_private_directory(
                        opened, label="strategy candidate snapshot directory"
                    )
                os.close(descriptor)
                descriptor = child
                child = -1
            return descriptor
        except OSError as exc:
            if child >= 0:
                os.close(child)
            if descriptor >= 0:
                os.close(descriptor)
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate snapshot directory is unsafe"
            ) from exc
        except BaseException:
            if child >= 0:
                os.close(child)
            if descriptor >= 0:
                os.close(descriptor)
            raise

    @classmethod
    def _open_child_directory(cls, parent_fd: int, name: str) -> int:
        descriptor = -1
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent_fd)
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except OSError as exc:
            if descriptor >= 0:
                os.close(descriptor)
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generations directory is missing or unsafe"
            ) from exc
        try:
            if not cls._same_file(before, opened) or not cls._same_file(opened, active):
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy candidate generations directory identity changed"
                )
            cls._validate_private_directory(
                opened, label="strategy candidate generations directory"
            )
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _initialize_for_publish(self) -> None:
        root_fd = self._open_or_create_private_directory(self.root)
        try:
            try:
                generations_fd = self._open_child_directory(root_fd, "generations")
            except StrategyCandidateSnapshotIntegrityError:
                with suppress(FileExistsError):
                    os.mkdir("generations", _PRIVATE_DIRECTORY_MODE, dir_fd=root_fd)
                generations_fd = self._open_child_directory(root_fd, "generations")
            os.close(generations_fd)
            self._ensure_lock_file(root_fd)
        finally:
            os.close(root_fd)

    @staticmethod
    def _validate_private_file(observed: os.stat_result, *, label: str) -> None:
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != _PRIVATE_FILE_MODE
        ):
            raise StrategyCandidateSnapshotIntegrityError(f"{label} is not a private regular file")

    @classmethod
    def _ensure_lock_file(cls, root_fd: int) -> None:
        flags = os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = -1
        try:
            try:
                descriptor = os.open(".publish.lock", flags, _PRIVATE_FILE_MODE, dir_fd=root_fd)
            except FileExistsError:
                descriptor = os.open(
                    ".publish.lock",
                    os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_fd,
                )
            cls._validate_private_file(os.fstat(descriptor), label="snapshot lock")
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate snapshot lock is unsafe"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @contextmanager
    def _locked(self, *, exclusive: bool) -> Iterator[tuple[int, int]]:
        with self._thread_lock:
            root_fd = self._open_directory(self.root, private_final=True)
            lock_fd = -1
            generations_fd = -1
            try:
                before = os.stat(".publish.lock", dir_fd=root_fd, follow_symlinks=False)
                if stat.S_ISLNK(before.st_mode):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot lock cannot be a symlink"
                    )
                lock_fd = os.open(
                    ".publish.lock",
                    (os.O_RDWR if exclusive else os.O_RDONLY) | getattr(os, "O_NOFOLLOW", 0),
                    dir_fd=root_fd,
                )
                opened = os.fstat(lock_fd)
                active = os.stat(".publish.lock", dir_fd=root_fd, follow_symlinks=False)
                self._validate_private_file(opened, label="snapshot lock")
                if not self._same_file(before, opened) or not self._same_file(opened, active):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot lock identity changed"
                    )
                fcntl.flock(lock_fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                locked_active = os.stat(".publish.lock", dir_fd=root_fd, follow_symlinks=False)
                if not self._same_file(opened, locked_active):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "strategy candidate snapshot lock changed while waiting"
                    )
                generations_fd = self._open_child_directory(root_fd, "generations")
                yield root_fd, generations_fd
            except OSError as exc:
                raise StrategyCandidateSnapshotIntegrityError(
                    "strategy candidate snapshot lock is missing or unsafe"
                ) from exc
            finally:
                if generations_fd >= 0:
                    os.close(generations_fd)
                if lock_fd >= 0:
                    with suppress(OSError):
                        fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)
                os.close(root_fd)

    def _read_all_generations(self, generations_fd: int) -> dict[int, StrategyCandidateSnapshot]:
        generations: dict[int, StrategyCandidateSnapshot] = {}
        observed_names: set[str] = set()
        try:
            with os.scandir(generations_fd) as entries:
                for index, entry in enumerate(entries, start=1):
                    if index > _MAX_GENERATIONS:
                        raise StrategyCandidateSnapshotIntegrityError(
                            "strategy candidate generation count exceeds limit"
                        )
                    match = re.fullmatch(r"([0-9a-f]{64})\.json", entry.name)
                    if match is None:
                        raise StrategyCandidateSnapshotIntegrityError(
                            "strategy candidate generations contain an unexpected entry"
                        )
                    observed = entry.stat(follow_symlinks=False)
                    if stat.S_ISLNK(observed.st_mode):
                        raise StrategyCandidateSnapshotIntegrityError(
                            "generation cannot be a symlink"
                        )
                    self._validate_private_file(observed, label="generation")
                    state = self._cache_state(observed)
                    cached_state = self._generation_states.get(entry.name)
                    if cached_state is None:
                        snapshot = self._read_snapshot(generations_fd, entry.name)
                        active = os.stat(
                            entry.name,
                            dir_fd=generations_fd,
                            follow_symlinks=False,
                        )
                        if self._cache_state(active) != state:
                            raise StrategyCandidateSnapshotIntegrityError(
                                "generation changed while populating cache"
                            )
                    else:
                        if cached_state != state:
                            raise StrategyCandidateSnapshotIntegrityError(
                                "immutable generation changed after validation"
                            )
                        self._validate_cached_generation(
                            generations_fd,
                            entry.name,
                            expected_state=state,
                        )
                        snapshot = self._generation_snapshots[entry.name]
                    observed_names.add(entry.name)
                    if snapshot.content_sha256 != match.group(1):
                        raise StrategyCandidateSnapshotIntegrityError(
                            "generation filename does not match content_sha256"
                        )
                    if snapshot.sequence in generations:
                        raise StrategyCandidateSnapshotIntegrityError(
                            "duplicate generation sequence conflict"
                        )
                    generations[snapshot.sequence] = snapshot
                    self._generation_states[entry.name] = state
                    self._generation_snapshots[entry.name] = snapshot
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generations are unreadable"
            ) from exc
        if set(self._generation_states) - observed_names:
            raise StrategyCandidateSnapshotIntegrityError(
                "generation sequence is missing a previously validated entry"
            )
        if generations and sorted(generations) != list(range(max(generations) + 1)):
            raise StrategyCandidateSnapshotIntegrityError(
                "generation sequence has a missing or conflicting entry"
            )
        return generations

    @staticmethod
    def _cache_state(observed: os.stat_result) -> tuple[int, ...]:
        return (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_uid,
            observed.st_nlink,
            observed.st_size,
            observed.st_mtime_ns,
            observed.st_ctime_ns,
        )

    @classmethod
    def _validate_cached_generation(
        cls,
        parent_fd: int,
        name: str,
        *,
        expected_state: tuple[int, ...],
    ) -> None:
        descriptor = -1
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            cls._validate_private_file(opened, label="generation")
            if (
                cls._cache_state(opened) != expected_state
                or cls._cache_state(active) != expected_state
            ):
                raise StrategyCandidateSnapshotIntegrityError(
                    "immutable generation changed after validation"
                )
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "cached generation is missing or unsafe"
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _validate_current_pointer(
        self,
        root_fd: int,
        generations: Mapping[int, StrategyCandidateSnapshot],
    ) -> None:
        pointer_exists = self._entry_exists(root_fd, "current.json")
        if not generations:
            if pointer_exists:
                raise StrategyCandidateSnapshotIntegrityError(
                    "current pointer generation is missing"
                )
            return
        if not pointer_exists:
            raise StrategyCandidateSnapshotIntegrityError("current pointer is missing")
        pointer = self._read_pointer(root_fd)
        latest = generations[max(generations)]
        expected = StrategyCandidateSnapshotPointer.from_snapshot(latest)
        if pointer != expected:
            if pointer.generation_sha256 not in {
                snapshot.content_sha256 for snapshot in generations.values()
            }:
                raise StrategyCandidateSnapshotIntegrityError(
                    "current pointer generation is missing"
                )
            raise StrategyCandidateSnapshotIntegrityError(
                "current pointer does not bind the latest generation"
            )

    def _finish_interrupted_publish(
        self,
        root_fd: int,
        generations: Mapping[int, StrategyCandidateSnapshot],
        snapshot: StrategyCandidateSnapshot,
    ) -> bool:
        if not generations or generations[max(generations)] != snapshot:
            return False
        if snapshot.sequence == 0:
            if self._entry_exists(root_fd, "current.json"):
                return False
        else:
            if not self._entry_exists(root_fd, "current.json"):
                return False
            pointer = self._read_pointer(root_fd)
            previous = generations.get(snapshot.sequence - 1)
            if previous is None or pointer != StrategyCandidateSnapshotPointer.from_snapshot(
                previous
            ):
                return False
        self._atomic_replace_pointer(
            root_fd,
            self._model_bytes(StrategyCandidateSnapshotPointer.from_snapshot(snapshot)),
        )
        return True

    def _read_snapshot(self, parent_fd: int, name: str) -> StrategyCandidateSnapshot:
        payload = self._read_regular_file(parent_fd, name, label="generation")
        try:
            raw = json.loads(payload)
            if not isinstance(raw, dict):
                raise ValueError("snapshot generation must be a JSON object")
            if "schema_version" not in raw:
                rows = raw.get("rows")
                if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
                    raise ValueError("legacy snapshot rows are invalid")
                if any("effective_trade_date" in row for row in rows):
                    raise ValueError("legacy snapshot cannot contain effective_trade_date")
                trade_date = raw.get("trade_date")
                raw["schema_version"] = 1
                for row in rows:
                    row["effective_trade_date"] = trade_date
                    row["legacy_utc_date_semantics"] = True
            snapshot = StrategyCandidateSnapshot.model_validate(raw)
        except (TypeError, ValueError) as exc:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generation is invalid"
            ) from exc
        if self._snapshot_bytes(snapshot) != payload:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate generation is not canonical JSON"
            )
        return snapshot

    @classmethod
    def _snapshot_bytes(cls, snapshot: StrategyCandidateSnapshot) -> bytes:
        payload = snapshot.model_dump(mode="json")
        if snapshot.schema_version == 1:
            payload.pop("schema_version")
            for row in payload["rows"]:
                row.pop("effective_trade_date")
        return cls._canonical_json_bytes(payload)

    def _read_pointer(self, root_fd: int) -> StrategyCandidateSnapshotPointer:
        payload = self._read_regular_file(root_fd, "current.json", label="current pointer")
        try:
            pointer = StrategyCandidateSnapshotPointer.model_validate_json(payload)
        except ValueError as exc:
            raise StrategyCandidateSnapshotIntegrityError("current pointer is invalid") from exc
        if self._model_bytes(pointer) != payload:
            raise StrategyCandidateSnapshotIntegrityError("current pointer is not canonical JSON")
        return pointer

    @staticmethod
    def _model_bytes(model: RuntimeContractModel) -> bytes:
        return StrategyCandidateSnapshotSpool._canonical_json_bytes(model.model_dump(mode="json"))

    @staticmethod
    def _canonical_json_bytes(value: object) -> bytes:
        payload = json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(payload) > _MAX_AUTHORITY_BYTES:
            raise StrategyCandidateSnapshotIntegrityError(
                "strategy candidate authority payload exceeds size limit"
            )
        return payload

    @classmethod
    def _read_regular_file(cls, parent_fd: int, name: str, *, label: str) -> bytes:
        descriptor = -1
        try:
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode):
                raise StrategyCandidateSnapshotIntegrityError(f"{label} cannot be a symlink")
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            cls._validate_private_file(opened, label=label)
            if not cls._same_file(before, opened) or not cls._same_file(opened, active):
                raise StrategyCandidateSnapshotIntegrityError(f"{label} identity changed")
            if opened.st_size > _MAX_AUTHORITY_BYTES:
                raise StrategyCandidateSnapshotIntegrityError(f"{label} exceeds size limit")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                payload = stream.read(_MAX_AUTHORITY_BYTES + 1)
            after = os.fstat(descriptor)
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if len(payload) > _MAX_AUTHORITY_BYTES:
                raise StrategyCandidateSnapshotIntegrityError(f"{label} exceeds size limit")
            if not cls._same_file(opened, after) or not cls._same_file(after, current):
                raise StrategyCandidateSnapshotIntegrityError(f"{label} changed while being read")
            if (opened.st_size, opened.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                raise StrategyCandidateSnapshotIntegrityError(f"{label} changed while being read")
            return payload
        except OSError as exc:
            raise StrategyCandidateSnapshotIntegrityError(f"{label} is missing or unsafe") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    @staticmethod
    def _entry_exists(parent_fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def _generation_name(content_sha256: str) -> str:
        if not _SHA256_PATTERN.fullmatch(content_sha256):
            raise StrategyCandidateSnapshotIntegrityError("generation content hash is invalid")
        return f"{content_sha256}.json"

    @classmethod
    def _write_temporary(cls, parent_fd: int, name: str, payload: bytes) -> None:
        descriptor = os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
            dir_fd=parent_fd,
        )
        try:
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @classmethod
    def _atomic_create_generation(
        cls,
        root_fd: int,
        generations_fd: int,
        target_name: str,
        payload: bytes,
    ) -> None:
        temporary_name = f".candidate-generation.{uuid4().hex}.tmp"
        try:
            cls._write_temporary(root_fd, temporary_name, payload)
            try:
                os.link(
                    temporary_name,
                    target_name,
                    src_dir_fd=root_fd,
                    dst_dir_fd=generations_fd,
                    follow_symlinks=False,
                )
            except FileExistsError as exc:
                raise StrategyCandidateSnapshotIntegrityError(
                    "immutable generation already exists"
                ) from exc
            os.fsync(generations_fd)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_fd)
            os.fsync(root_fd)

    @classmethod
    def _atomic_replace_pointer(cls, root_fd: int, payload: bytes) -> None:
        temporary_name = f".current.{uuid4().hex}.tmp"
        try:
            cls._write_temporary(root_fd, temporary_name, payload)
            os.replace(
                temporary_name,
                "current.json",
                src_dir_fd=root_fd,
                dst_dir_fd=root_fd,
            )
            os.fsync(root_fd)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary_name, dir_fd=root_fd)

    @classmethod
    def _cleanup_stale_temporaries(cls, root_fd: int) -> None:
        pattern = re.compile(r"^\.(?:candidate-generation|current)\.[0-9a-f]{32}\.tmp$")
        with os.scandir(root_fd) as entries:
            for entry in entries:
                if pattern.fullmatch(entry.name) is None:
                    continue
                observed = os.stat(entry.name, dir_fd=root_fd, follow_symlinks=False)
                if (
                    not stat.S_ISREG(observed.st_mode)
                    or observed.st_uid != os.getuid()
                    or observed.st_nlink not in {1, 2}
                    or stat.S_IMODE(observed.st_mode) != _PRIVATE_FILE_MODE
                ):
                    raise StrategyCandidateSnapshotIntegrityError(
                        "stale publish temporary is unsafe"
                    )
                os.unlink(entry.name, dir_fd=root_fd)
        os.fsync(root_fd)
