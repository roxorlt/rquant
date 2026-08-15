"""Durable, fenced stage ledger for the isolated daily-close pipeline."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Self
from urllib.parse import quote

from pydantic import Field, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

_SCHEMA_VERSION = 4
_MAX_ERROR_MESSAGE = 4_096
_MAX_STAGE_COUNT = 64
_MAX_BACKOFF_SECONDS = 24 * 60 * 60
_DEFAULT_RECOVERY_LIMIT = 128
_MAX_RECOVERY_LIMIT = 1_024
_DEFAULT_DEADLINE_SWEEP_LIMIT = 128
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]


class DailyPipelineLedgerError(RuntimeError):
    """The durable ledger is unavailable, stale, or internally inconsistent."""


class LeaseLost(DailyPipelineLedgerError):  # noqa: N818
    """The caller no longer owns the writer lease or its fencing authority."""


class _CommittedLedgerError(DailyPipelineLedgerError):
    """Internal error for business-rule rejections after durable state was committed."""


class DailyStageState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DailyRunState(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DailyPipelineMode(StrEnum):
    SHADOW = "shadow"
    PRODUCTION = "production"


class DailyPipelineStorageProfile(RuntimeContractModel):
    """Physical daily-DAG namespace bound to one mode and runtime profile."""

    contract: Literal["daily-pipeline-storage-profile/v1"] = "daily-pipeline-storage-profile/v1"
    root: Path
    mode: DailyPipelineMode
    profile_hash: Sha256
    namespace_id: Sha256 | None = None

    @field_validator("root")
    @classmethod
    def validate_root(cls, value: Path) -> Path:
        candidate = Path(value)
        normalized = Path(os.path.normpath(os.fspath(candidate)))
        if not candidate.is_absolute() or candidate != normalized:
            raise ValueError("daily pipeline storage root must be absolute and normalized")
        return candidate

    @model_validator(mode="after")
    def bind_namespace(self) -> Self:
        expected = canonical_sha256(
            {
                "contract": "daily-pipeline-storage-namespace/v1",
                "mode": self.mode,
                "profile_hash": self.profile_hash,
            }
        )
        if self.namespace_id is None:
            object.__setattr__(self, "namespace_id", expected)
        elif self.namespace_id != expected:
            raise ValueError("daily pipeline storage namespace does not match its profile")
        return self

    @classmethod
    def create(
        cls,
        *,
        root: Path,
        mode: DailyPipelineMode,
        profile_hash: str,
    ) -> DailyPipelineStorageProfile:
        return cls(root=root, mode=mode, profile_hash=profile_hash)

    @property
    def namespace_root(self) -> Path:
        return self.root / self.mode.value / str(self.namespace_id)

    @property
    def state_path(self) -> Path:
        return self.namespace_root / "state" / "daily-pipeline.sqlite3"

    @property
    def receipt_root(self) -> Path:
        return self.namespace_root / "receipts"

    @property
    def report_root(self) -> Path:
        return self.namespace_root / "reports"

    @property
    def command_manifest_path(self) -> Path:
        return self.namespace_root / "control" / "command-manifest.json"


_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_CLOEXEC", 0)
)
_REGULAR_FILE_OPEN_FLAGS = (
    os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
)


@dataclass(frozen=True)
class DailyPipelinePathIdentity:
    path: Path
    device: int
    inode: int
    owner: int
    mode: int

    @classmethod
    def from_stat(cls, path: Path, observed: os.stat_result) -> DailyPipelinePathIdentity:
        return cls(
            path=path,
            device=observed.st_dev,
            inode=observed.st_ino,
            owner=observed.st_uid,
            mode=observed.st_mode,
        )

    def matches(self, observed: os.stat_result) -> bool:
        return (
            self.device,
            self.inode,
            self.owner,
            self.mode,
        ) == (
            observed.st_dev,
            observed.st_ino,
            observed.st_uid,
            observed.st_mode,
        )


class DailyPipelineStorageBinding:
    """Descriptor-bound access to one profile leaf with a verified directory chain."""

    _LEAVES = ("state", "receipts", "reports", "control")

    def __init__(
        self,
        *,
        profile: DailyPipelineStorageProfile,
        leaf: str,
        leaf_fd: int,
        identities: tuple[DailyPipelinePathIdentity, ...],
    ) -> None:
        self.profile = profile
        self.leaf = leaf
        self.leaf_fd = leaf_fd
        self.identities = identities
        self._closed = False

    @classmethod
    def open(
        cls,
        profile: DailyPipelineStorageProfile,
        *,
        leaf: Literal["state", "receipts", "reports", "control"],
    ) -> DailyPipelineStorageBinding:
        verified = DailyPipelineStorageProfile.model_validate(
            profile.model_dump(mode="python")
            if isinstance(profile, DailyPipelineStorageProfile)
            else profile
        )
        descriptors: list[int] = []
        identities: list[DailyPipelinePathIdentity] = []
        selected_fd = -1
        try:
            parent_fd = os.open("/", _DIRECTORY_OPEN_FLAGS)
            descriptors.append(parent_fd)
            current = Path("/")
            identities.append(DailyPipelinePathIdentity.from_stat(current, os.fstat(parent_fd)))
            for component in verified.root.parts[1:]:
                current /= component
                parent_fd = _open_bound_directory(
                    parent_fd,
                    component,
                    path=current,
                    create=False,
                    controlled=False,
                )
                descriptors.append(parent_fd)
                identities.append(DailyPipelinePathIdentity.from_stat(current, os.fstat(parent_fd)))
            for component in (verified.mode.value, str(verified.namespace_id)):
                current /= component
                parent_fd = _open_bound_directory(
                    parent_fd,
                    component,
                    path=current,
                    create=True,
                    controlled=True,
                )
                descriptors.append(parent_fd)
                identities.append(DailyPipelinePathIdentity.from_stat(current, os.fstat(parent_fd)))
            namespace_fd = parent_fd
            for name in cls._LEAVES:
                child_path = current / name
                child_fd = _open_bound_directory(
                    namespace_fd,
                    name,
                    path=child_path,
                    create=True,
                    controlled=True,
                )
                identities.append(
                    DailyPipelinePathIdentity.from_stat(child_path, os.fstat(child_fd))
                )
                if name == leaf:
                    selected_fd = child_fd
                else:
                    os.close(child_fd)
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            descriptors.clear()
            binding = cls(
                profile=verified,
                leaf=leaf,
                leaf_fd=selected_fd,
                identities=tuple(identities),
            )
            binding.assert_current()
            return binding
        except (OSError, DailyPipelineLedgerError) as exc:
            if selected_fd >= 0:
                os.close(selected_fd)
            for descriptor in reversed(descriptors):
                with suppress(OSError):
                    os.close(descriptor)
            if isinstance(exc, DailyPipelineLedgerError):
                raise
            raise DailyPipelineLedgerError(
                "daily pipeline storage binding is unsafe or unavailable"
            ) from exc

    @property
    def declared_root(self) -> Path:
        return self.profile.namespace_root / self.leaf

    def assert_current(self) -> None:
        if self._closed:
            raise DailyPipelineLedgerError("daily pipeline storage binding is closed")
        for identity in self.identities:
            try:
                observed = identity.path.lstat()
            except OSError as exc:
                raise DailyPipelineLedgerError(
                    "daily pipeline storage binding changed or disappeared"
                ) from exc
            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                raise DailyPipelineLedgerError(
                    "daily pipeline storage binding contains an unsafe path"
                )
            if not identity.matches(observed):
                raise DailyPipelineLedgerError("daily pipeline storage binding was replaced")
        observed_leaf = os.fstat(self.leaf_fd)
        expected_leaf = next(
            identity for identity in self.identities if identity.path == self.declared_root
        )
        if not expected_leaf.matches(observed_leaf):
            raise DailyPipelineLedgerError("daily pipeline storage leaf binding was replaced")

    def open_regular_file(self, name: str) -> tuple[int, DailyPipelinePathIdentity]:
        if not name or "/" in name or name in {".", ".."}:
            raise DailyPipelineLedgerError("daily pipeline storage filename is invalid")
        self.assert_current()
        try:
            descriptor = os.open(
                name,
                _REGULAR_FILE_OPEN_FLAGS,
                0o600,
                dir_fd=self.leaf_fd,
            )
            observed = os.fstat(descriptor)
        except OSError as exc:
            raise DailyPipelineLedgerError(
                "daily pipeline storage file is unsafe or unavailable"
            ) from exc
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or stat.S_IMODE(observed.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise DailyPipelineLedgerError("daily pipeline storage file identity is unsafe")
        return descriptor, DailyPipelinePathIdentity.from_stat(
            self.declared_root / name,
            observed,
        )

    def assert_regular_file(self, name: str, identity: DailyPipelinePathIdentity) -> None:
        try:
            observed = os.stat(name, dir_fd=self.leaf_fd, follow_symlinks=False)
        except OSError as exc:
            raise DailyPipelineLedgerError(
                "daily pipeline storage file changed or disappeared"
            ) from exc
        if not identity.matches(observed) or not stat.S_ISREG(observed.st_mode):
            raise DailyPipelineLedgerError("daily pipeline storage file was replaced")

    def sqlite_target(self, name: str) -> tuple[str, bool]:
        proc_root = Path(f"/proc/self/fd/{self.leaf_fd}")
        if proc_root.is_dir():
            return str(proc_root / name), False
        encoded = quote(str(self.declared_root / name), safe="/")
        return f"file:{encoded}?mode=rw", True

    def close(self) -> None:
        if not self._closed:
            os.close(self.leaf_fd)
            self._closed = True

    def __enter__(self) -> DailyPipelineStorageBinding:
        return self

    def __exit__(self, *_error: object) -> None:
        self.close()

    def __del__(self) -> None:
        if not getattr(self, "_closed", True):
            with suppress(OSError):
                self.close()


def _open_bound_directory(
    parent_fd: int,
    name: str,
    *,
    path: Path,
    create: bool,
    controlled: bool,
) -> int:
    if not name or "/" in name or name in {".", ".."}:
        raise DailyPipelineLedgerError("daily pipeline storage component is invalid")
    try:
        descriptor = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_fd)
    except FileNotFoundError:
        if not create:
            raise DailyPipelineLedgerError(
                f"daily pipeline declared storage root is missing: {path}"
            ) from None
        with suppress(FileExistsError):
            os.mkdir(name, 0o700, dir_fd=parent_fd)
        descriptor = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent_fd)
    observed = os.fstat(descriptor)
    if not stat.S_ISDIR(observed.st_mode):
        os.close(descriptor)
        raise DailyPipelineLedgerError("daily pipeline storage component is not a directory")
    if controlled and (observed.st_uid != os.geteuid() or stat.S_IMODE(observed.st_mode) != 0o700):
        os.close(descriptor)
        raise DailyPipelineLedgerError(
            "daily pipeline controlled storage directory has unsafe owner or mode"
        )
    return descriptor


class DailyStageSpec(RuntimeContractModel):
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    depends_on: tuple[str, ...] = ()
    max_attempts: int = Field(default=3, ge=1, le=20)
    retry_backoff_seconds: int = Field(default=30, ge=0, le=_MAX_BACKOFF_SECONDS)
    deadline_at: AwareUtcDatetime | None = None

    @field_validator("depends_on")
    @classmethod
    def validate_dependencies(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if len(values) != len(set(values)):
            raise ValueError("stage dependencies must be unique")
        if any(not value or len(value) > 64 for value in values):
            raise ValueError("stage dependency ids are invalid")
        return tuple(values)

    @model_validator(mode="after")
    def validate_not_self_dependent(self) -> Self:
        if self.stage_id in self.depends_on:
            raise ValueError("stage cannot depend on itself")
        return self


class DailyRunSpec(RuntimeContractModel):
    contract: Literal["daily-run-spec/v3"] = "daily-run-spec/v3"
    run_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    mode: DailyPipelineMode
    trade_date: date
    source_generation_id: Sha256
    source_content_hash: Sha256
    command_manifest_hash: Sha256
    code_commit: CommitSha
    profile_hash: Sha256
    deadline_at: AwareUtcDatetime | None = None
    stages: tuple[DailyStageSpec, ...] = Field(min_length=1, max_length=_MAX_STAGE_COUNT)

    @field_validator("stages")
    @classmethod
    def validate_stages(cls, values: tuple[DailyStageSpec, ...]) -> tuple[DailyStageSpec, ...]:
        ids = tuple(stage.stage_id for stage in values)
        if len(ids) != len(set(ids)):
            raise ValueError("daily stages must have unique ids")
        available = set(ids)
        if any(dependency not in available for stage in values for dependency in stage.depends_on):
            raise ValueError("daily stage dependency is not declared")
        _assert_acyclic(values)
        return values

    @model_validator(mode="after")
    def bind_run_identity(self) -> Self:
        if self.deadline_at is not None:
            object.__setattr__(self, "deadline_at", normalize_aware_utc(self.deadline_at))
        if self.run_id is None:
            identity = self.model_dump(mode="python", exclude={"run_id"})
            object.__setattr__(self, "run_id", f"daily-{canonical_sha256(identity)[:40]}")
        return self

    @property
    def input_identity(self) -> Sha256:
        return canonical_sha256(
            {
                "source_generation_id": self.source_generation_id,
                "source_content_hash": self.source_content_hash,
                "mode": self.mode,
                "command_manifest_hash": self.command_manifest_hash,
                "code_commit": self.code_commit,
                "profile_hash": self.profile_hash,
            }
        )


class DailyWriterLease(RuntimeContractModel):
    service_owner: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    fencing_token: int = Field(ge=1)
    acquired_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_window(self) -> Self:
        if self.expires_at <= self.acquired_at:
            raise ValueError("writer lease must expire after acquisition")
        return self


class DailyStageAttempt(RuntimeContractModel):
    mode: DailyPipelineMode = DailyPipelineMode.SHADOW
    run_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    attempt_number: int = Field(ge=1)
    fencing_token: int = Field(ge=1)
    claimed_at: AwareUtcDatetime
    lease_expires_at: AwareUtcDatetime


class DailyStageEffectIntent(RuntimeContractModel):
    """Deterministic external-effect reservation owned by one daily stage.

    The idempotency key is intentionally independent from a ledger attempt
    number.  A process restart may fence and adopt an unfinished attempt, but
    it must address the same external operation rather than create a second
    side effect.
    """

    contract: Literal["daily-stage-effect-intent/v3"] = "daily-stage-effect-intent/v3"
    effect_id: Sha256 | None = None
    mode: DailyPipelineMode
    idempotency_key: Sha256
    command_manifest_hash: Sha256
    adapter_identity: str = Field(min_length=1, max_length=256)
    receipt_locator: str = Field(min_length=1, max_length=4_096)

    @model_validator(mode="after")
    def bind_effect_identity(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"effect_id"}))
        if self.effect_id is None:
            object.__setattr__(self, "effect_id", expected)
        elif self.effect_id != expected:
            raise ValueError("daily stage effect id does not match canonical content")
        return self


class DailyStageEffectRecord(RuntimeContractModel):
    """Durable ledger binding between a claimed attempt and an effect intent."""

    run_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    mode: DailyPipelineMode
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    attempt_number: int = Field(ge=1)
    intent: DailyStageEffectIntent
    prepared_at: AwareUtcDatetime


class DailyExternalEffectFence(RuntimeContractModel):
    """Transaction-held identities authorized to publish an external receipt."""

    run_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    mode: DailyPipelineMode
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    idempotency_key: Sha256
    fencing_token: int = Field(ge=1)
    lease_expiry: AwareUtcDatetime
    source_generation_id: Sha256
    source_content_hash: Sha256
    command_manifest_hash: Sha256
    effect_id: Sha256


class StageResult(RuntimeContractModel):
    content_hash: Sha256
    evidence_hash: Sha256


class StageFailure(RuntimeContractModel):
    error_code: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    message: str = Field(min_length=1, max_length=_MAX_ERROR_MESSAGE)


class DailyStageReceipt(RuntimeContractModel):
    contract: Literal["daily-stage-receipt/v2"] = "daily-stage-receipt/v2"
    receipt_id: Sha256 | None = None
    mode: DailyPipelineMode
    run_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    attempt_number: int = Field(ge=1)
    input_identity: Sha256
    result: StageResult
    prepared_at: AwareUtcDatetime

    @model_validator(mode="after")
    def bind_identity(self) -> Self:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"receipt_id"}))
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("daily stage receipt id does not match canonical content")
        return self


class DailyStageRecord(RuntimeContractModel):
    run_id: str
    stage_id: str
    sequence: int = Field(ge=0)
    state: DailyStageState
    attempts: int = Field(ge=0)
    next_attempt_at: AwareUtcDatetime | None = None
    claimed_at: AwareUtcDatetime | None = None
    claim_fencing_token: int | None = Field(default=None, ge=1)
    claim_lease_expires_at: AwareUtcDatetime | None = None
    terminal_receipt_id: Sha256 | None = None
    last_failure: StageFailure | None = None


class DailyRunRecord(RuntimeContractModel):
    run_id: str
    spec: DailyRunSpec
    input_identity: Sha256
    state: DailyRunState
    created_at: AwareUtcDatetime
    cancelled_reason: str | None = None


class DailyRecoverySummary(RuntimeContractModel):
    finalized_receipt_ids: tuple[Sha256, ...] = ()
    retried_stage_ids: tuple[str, ...] = ()
    failed_stage_ids: tuple[str, ...] = ()
    next_cursor: str | None = None


class DailyLedgerStageFence:
    """Public, transaction-held view of one claimed daily stage."""

    def __init__(
        self,
        *,
        ledger: DailyPipelineLedger,
        connection: sqlite3.Connection,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
    ) -> None:
        self._ledger = ledger
        self._connection = connection
        self._lease = DailyWriterLease.model_validate(lease)
        self._attempt = DailyStageAttempt.model_validate(attempt)

    def assert_current(self, checked_at: datetime, /) -> None:
        observed = normalize_aware_utc(checked_at)
        self._ledger._validate_lease(self._connection, self._lease, observed)
        run = self._ledger._run_row(self._connection, self._attempt.run_id)
        if run["state"] != DailyRunState.RUNNING.value:
            raise DailyPipelineLedgerError("daily stage run is not active")
        stage = self._ledger._stage_row(
            self._connection,
            self._attempt.run_id,
            self._attempt.stage_id,
        )
        self._ledger._assert_active_attempt(stage, self._attempt, self._lease)

    def assert_source(
        self,
        source_generation_id: str,
        source_content_hash: str,
        /,
    ) -> None:
        run = self._ledger._run_from_row(
            self._ledger._run_row(self._connection, self._attempt.run_id)
        )
        if (
            run.spec.source_generation_id != source_generation_id
            or run.spec.source_content_hash != source_content_hash
        ):
            raise DailyPipelineLedgerError("daily stage source identity is stale")

    def assert_input(self, input_identity: str, /) -> None:
        run = self._ledger._run_from_row(
            self._ledger._run_row(self._connection, self._attempt.run_id)
        )
        if run.input_identity != input_identity:
            raise DailyPipelineLedgerError("daily stage input identity is stale")


def _assert_acyclic(stages: tuple[DailyStageSpec, ...]) -> None:
    dependencies = {stage.stage_id: set(stage.depends_on) for stage in stages}
    completed: set[str] = set()
    while dependencies:
        ready = {stage_id for stage_id, values in dependencies.items() if values <= completed}
        if not ready:
            raise ValueError("daily stage graph contains a cycle")
        completed.update(ready)
        for stage_id in ready:
            dependencies.pop(stage_id)


class DailyPipelineLedger:
    """SQLite authority for a single daily-close service owner.

    The ledger has one physical writer at a time.  Every mutation verifies the
    service's stable owner and current fencing token before it changes state.
    """

    def __init__(
        self,
        *,
        storage_profile: DailyPipelineStorageProfile,
        service_owner: str,
    ) -> None:
        if not service_owner or len(service_owner) > 128:
            raise ValueError("service_owner is invalid")
        self.storage_profile = DailyPipelineStorageProfile.model_validate(storage_profile)
        self.path = self.storage_profile.state_path
        self.service_owner = service_owner
        self._initialize()

    def _connect(self) -> tuple[sqlite3.Connection, DailyPipelineStorageBinding]:
        binding = DailyPipelineStorageBinding.open(self.storage_profile, leaf="state")
        file_descriptor = -1
        connection: sqlite3.Connection | None = None
        try:
            file_descriptor, file_identity = binding.open_regular_file(self.path.name)
            target, uri = binding.sqlite_target(self.path.name)
            connection = sqlite3.connect(
                target,
                timeout=5.0,
                isolation_level=None,
                uri=uri,
            )
            binding.assert_current()
            binding.assert_regular_file(self.path.name, file_identity)
        except sqlite3.Error as exc:
            if connection is not None:
                connection.close()
            try:
                binding.assert_current()
            except DailyPipelineLedgerError as binding_error:
                binding.close()
                raise binding_error from exc
            binding.close()
            raise DailyPipelineLedgerError("unable to open daily pipeline ledger") from exc
        except DailyPipelineLedgerError:
            if connection is not None:
                connection.close()
            binding.close()
            raise
        finally:
            if file_descriptor >= 0:
                os.close(file_descriptor)
        assert connection is not None
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("PRAGMA synchronous = FULL")
            mode = str(connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]).lower()
            if mode != "wal":
                raise DailyPipelineLedgerError("daily pipeline ledger requires SQLite WAL")
        except sqlite3.Error as exc:
            connection.close()
            binding.close()
            raise DailyPipelineLedgerError("unable to configure daily pipeline ledger") from exc
        try:
            binding.assert_current()
            binding.assert_regular_file(self.path.name, file_identity)
        except DailyPipelineLedgerError:
            connection.close()
            binding.close()
            raise
        return connection, binding

    def _initialize(self) -> None:
        with self._transaction() as connection:
            present = connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name = 'daily_pipeline_meta'"
            ).fetchone()
            if present is None:
                self._create_schema(connection)
                connection.execute(
                    "INSERT INTO daily_pipeline_meta(key, value) VALUES (?, ?)",
                    ("schema_version", str(_SCHEMA_VERSION)),
                )
                connection.execute(
                    "INSERT INTO daily_pipeline_meta(key, value) VALUES (?, ?)",
                    ("service_owner", self.service_owner),
                )
                connection.executemany(
                    "INSERT INTO daily_pipeline_meta(key, value) VALUES (?, ?)",
                    (
                        ("mode", self.storage_profile.mode.value),
                        ("profile_hash", self.storage_profile.profile_hash),
                        ("namespace_id", self.storage_profile.namespace_id),
                    ),
                )
                connection.execute(
                    "INSERT INTO daily_pipeline_writer("
                    "singleton, service_owner, fencing_token"
                    ") VALUES (1, ?, 0)",
                    (self.service_owner,),
                )
                return
            metadata = self._metadata(connection)
            try:
                version = int(metadata["schema_version"])
            except (KeyError, ValueError) as exc:
                raise DailyPipelineLedgerError("daily pipeline ledger metadata is corrupt") from exc
            if version == 0:
                self._migrate_v0_to_v1(connection, metadata)
                version = 1
            if version == 1:
                self._migrate_v1_to_v2(connection, metadata)
                version = 2
            if version == 2:
                self._migrate_v2_to_v3(connection, metadata)
                version = 3
            if version == 3:
                self._migrate_v3_to_v4(connection, self._metadata(connection))
            self._ensure_receipt_integrity_columns(connection)
            self._validate_schema(connection)

    @staticmethod
    def _create_schema(connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE daily_pipeline_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE daily_pipeline_writer (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                service_owner TEXT NOT NULL,
                fencing_token INTEGER NOT NULL,
                lease_expires_at TEXT,
                acquired_at TEXT
            );
            CREATE TABLE daily_pipeline_run (
                run_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                profile_hash TEXT NOT NULL,
                namespace_id TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                spec_hash TEXT NOT NULL,
                input_identity TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                cancelled_reason TEXT
            );
            CREATE TABLE daily_pipeline_stage (
                run_id TEXT NOT NULL REFERENCES daily_pipeline_run(run_id),
                stage_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,
                retry_backoff_seconds INTEGER NOT NULL,
                deadline_at TEXT,
                next_attempt_at TEXT,
                claimed_at TEXT,
                claim_fencing_token INTEGER,
                claim_lease_expires_at TEXT,
                terminal_receipt_id TEXT,
                failure_code TEXT,
                failure_message TEXT,
                PRIMARY KEY (run_id, stage_id)
            );
            CREATE TABLE daily_pipeline_dependency (
                run_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                dependency_stage_id TEXT NOT NULL,
                PRIMARY KEY (run_id, stage_id, dependency_stage_id),
                FOREIGN KEY (run_id, stage_id)
                    REFERENCES daily_pipeline_stage(run_id, stage_id)
            );
            CREATE TABLE daily_pipeline_stage_receipt (
                receipt_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                profile_hash TEXT NOT NULL,
                namespace_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                evidence_hash TEXT NOT NULL,
                receipt_json TEXT NOT NULL,
                UNIQUE (run_id, stage_id, attempt_number),
                FOREIGN KEY (run_id, stage_id)
                    REFERENCES daily_pipeline_stage(run_id, stage_id)
            );
            CREATE TABLE daily_pipeline_effect_intent (
                run_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                profile_hash TEXT NOT NULL,
                namespace_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                effect_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                adapter_identity TEXT NOT NULL,
                receipt_locator TEXT NOT NULL,
                intent_json TEXT NOT NULL,
                prepared_at TEXT NOT NULL,
                PRIMARY KEY (run_id, stage_id),
                UNIQUE (effect_id),
                UNIQUE (idempotency_key),
                FOREIGN KEY (run_id, stage_id)
                    REFERENCES daily_pipeline_stage(run_id, stage_id)
            );
            CREATE TABLE daily_pipeline_schema_migration (
                migration_id TEXT PRIMARY KEY,
                from_version INTEGER NOT NULL,
                to_version INTEGER NOT NULL,
                applied_at TEXT NOT NULL
            );
            CREATE INDEX daily_pipeline_ready_idx
                ON daily_pipeline_stage(state, next_attempt_at, sequence);
            """
        )

    @staticmethod
    def _metadata(connection: sqlite3.Connection) -> dict[str, str]:
        rows = connection.execute("SELECT key, value FROM daily_pipeline_meta").fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    def _migrate_v0_to_v1(
        self,
        connection: sqlite3.Connection,
        metadata: dict[str, str],
    ) -> None:
        if metadata.get("service_owner") != self.service_owner:
            raise DailyPipelineLedgerError("daily pipeline ledger stable service owner mismatch")
        required = {
            "daily_pipeline_writer",
            "daily_pipeline_run",
            "daily_pipeline_stage",
            "daily_pipeline_dependency",
            "daily_pipeline_stage_receipt",
        }
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        tables = {str(row["name"]) for row in rows}
        if not required <= tables:
            raise DailyPipelineLedgerError("daily pipeline v0 schema is incomplete")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_pipeline_schema_migration (
                migration_id TEXT PRIMARY KEY,
                from_version INTEGER NOT NULL,
                to_version INTEGER NOT NULL,
                applied_at TEXT NOT NULL
            )
            """
        )
        applied_at = datetime.now().astimezone()
        migration_id = canonical_sha256(
            {
                "contract": "daily-pipeline-schema-migration/v1",
                "from_version": 0,
                "to_version": 1,
                "service_owner": self.service_owner,
            }
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO daily_pipeline_schema_migration(
                migration_id, from_version, to_version, applied_at
            ) VALUES (?, 0, ?, ?)
            """,
            (migration_id, 1, _dump_datetime(applied_at)),
        )
        connection.execute(
            "UPDATE daily_pipeline_meta SET value = ? WHERE key = 'schema_version'",
            ("1",),
        )

    def _migrate_v1_to_v2(
        self,
        connection: sqlite3.Connection,
        metadata: dict[str, str],
    ) -> None:
        if metadata.get("service_owner") != self.service_owner:
            raise DailyPipelineLedgerError("daily pipeline ledger stable service owner mismatch")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_pipeline_effect_intent (
                run_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                effect_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                adapter_identity TEXT NOT NULL,
                receipt_locator TEXT NOT NULL,
                intent_json TEXT NOT NULL,
                prepared_at TEXT NOT NULL,
                PRIMARY KEY (run_id, stage_id),
                UNIQUE (effect_id),
                UNIQUE (idempotency_key),
                FOREIGN KEY (run_id, stage_id)
                    REFERENCES daily_pipeline_stage(run_id, stage_id)
            )
            """
        )
        applied_at = datetime.now().astimezone()
        migration_id = canonical_sha256(
            {
                "contract": "daily-pipeline-schema-migration/v1",
                "from_version": 1,
                "to_version": 2,
                "service_owner": self.service_owner,
            }
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO daily_pipeline_schema_migration(
                migration_id, from_version, to_version, applied_at
            ) VALUES (?, 1, 2, ?)
            """,
            (migration_id, _dump_datetime(applied_at)),
        )
        connection.execute(
            "UPDATE daily_pipeline_meta SET value = '2' WHERE key = 'schema_version'"
        )

    def _migrate_v2_to_v3(
        self,
        connection: sqlite3.Connection,
        metadata: dict[str, str],
    ) -> None:
        if metadata.get("service_owner") != self.service_owner:
            raise DailyPipelineLedgerError("daily pipeline ledger stable service owner mismatch")
        for row in connection.execute("SELECT spec_json FROM daily_pipeline_run"):
            try:
                payload = json.loads(row["spec_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise DailyPipelineLedgerError("daily pipeline run spec is corrupt") from exc
            if payload.get("contract") != "daily-run-spec/v2" or not payload.get(
                "command_manifest_hash"
            ):
                raise DailyPipelineLedgerError(
                    "daily pipeline v2 state lacks an immutable command manifest binding"
                )
        for row in connection.execute("SELECT intent_json FROM daily_pipeline_effect_intent"):
            try:
                payload = json.loads(row["intent_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise DailyPipelineLedgerError("daily stage effect intent is corrupt") from exc
            if payload.get("contract") != "daily-stage-effect-intent/v2" or not payload.get(
                "command_manifest_hash"
            ):
                raise DailyPipelineLedgerError(
                    "daily pipeline v2 effect lacks an immutable command manifest binding"
                )
        applied_at = datetime.now().astimezone()
        migration_id = canonical_sha256(
            {
                "contract": "daily-pipeline-schema-migration/v1",
                "from_version": 2,
                "to_version": 3,
                "service_owner": self.service_owner,
            }
        )
        connection.execute(
            """
            INSERT OR IGNORE INTO daily_pipeline_schema_migration(
                migration_id, from_version, to_version, applied_at
            ) VALUES (?, 2, 3, ?)
            """,
            (migration_id, _dump_datetime(applied_at)),
        )
        connection.execute(
            "UPDATE daily_pipeline_meta SET value = '3' WHERE key = 'schema_version'"
        )

    def _migrate_v3_to_v4(
        self,
        connection: sqlite3.Connection,
        metadata: dict[str, str],
    ) -> None:
        if metadata.get("service_owner") != self.service_owner:
            raise DailyPipelineLedgerError("daily pipeline ledger stable service owner mismatch")
        protected_tables = (
            "daily_pipeline_run",
            "daily_pipeline_stage_receipt",
            "daily_pipeline_effect_intent",
        )
        if any(
            int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) > 0
            for table in protected_tables
        ):
            raise DailyPipelineLedgerError(
                "daily pipeline v3 state lacks native mode and cannot be migrated safely"
            )
        mode = self.storage_profile.mode.value
        profile_hash = self.storage_profile.profile_hash
        namespace_id = str(self.storage_profile.namespace_id)
        for table in protected_tables:
            connection.execute(
                f"ALTER TABLE {table} ADD COLUMN mode TEXT NOT NULL DEFAULT '{mode}'"
            )
            connection.execute(
                f"ALTER TABLE {table} ADD COLUMN profile_hash TEXT NOT NULL "
                f"DEFAULT '{profile_hash}'"
            )
            connection.execute(
                f"ALTER TABLE {table} ADD COLUMN namespace_id TEXT NOT NULL "
                f"DEFAULT '{namespace_id}'"
            )
        connection.executemany(
            "INSERT INTO daily_pipeline_meta(key, value) VALUES (?, ?)",
            (
                ("mode", mode),
                ("profile_hash", profile_hash),
                ("namespace_id", namespace_id),
            ),
        )
        applied_at = datetime.now().astimezone()
        migration_id = canonical_sha256(
            {
                "contract": "daily-pipeline-schema-migration/v1",
                "from_version": 3,
                "to_version": 4,
                "service_owner": self.service_owner,
                "mode": mode,
                "profile_hash": profile_hash,
                "namespace_id": namespace_id,
            }
        )
        connection.execute(
            """
            INSERT INTO daily_pipeline_schema_migration(
                migration_id, from_version, to_version, applied_at
            ) VALUES (?, 3, 4, ?)
            """,
            (migration_id, _dump_datetime(applied_at)),
        )
        connection.execute(
            "UPDATE daily_pipeline_meta SET value = '4' WHERE key = 'schema_version'"
        )

    def _validate_schema(self, connection: sqlite3.Connection) -> None:
        try:
            metadata = self._metadata(connection)
            version = int(metadata["schema_version"])
        except (KeyError, ValueError, sqlite3.Error) as exc:
            raise DailyPipelineLedgerError("daily pipeline ledger metadata is corrupt") from exc
        if version != _SCHEMA_VERSION:
            raise DailyPipelineLedgerError(
                f"unsupported schema version {version}; expected {_SCHEMA_VERSION}"
            )
        if metadata.get("service_owner") != self.service_owner:
            raise DailyPipelineLedgerError("daily pipeline ledger stable service owner mismatch")
        expected_profile = {
            "mode": self.storage_profile.mode.value,
            "profile_hash": self.storage_profile.profile_hash,
            "namespace_id": str(self.storage_profile.namespace_id),
        }
        if any(metadata.get(key) != value for key, value in expected_profile.items()):
            raise DailyPipelineLedgerError("daily pipeline ledger storage profile mismatch")
        self._validate_required_tables(connection)
        writer = connection.execute(
            "SELECT service_owner FROM daily_pipeline_writer WHERE singleton = 1"
        ).fetchone()
        if writer is None or writer["service_owner"] != self.service_owner:
            raise DailyPipelineLedgerError("daily pipeline ledger writer authority is corrupt")

    @staticmethod
    def _validate_required_tables(connection: sqlite3.Connection) -> None:
        required_columns = {
            "daily_pipeline_writer": {"singleton", "service_owner", "fencing_token"},
            "daily_pipeline_run": {
                "run_id",
                "mode",
                "profile_hash",
                "namespace_id",
                "spec_json",
                "spec_hash",
                "input_identity",
                "state",
                "created_at",
            },
            "daily_pipeline_stage": {
                "run_id",
                "stage_id",
                "sequence",
                "state",
                "attempts",
                "max_attempts",
                "retry_backoff_seconds",
            },
            "daily_pipeline_dependency": {"run_id", "stage_id", "dependency_stage_id"},
            "daily_pipeline_stage_receipt": {
                "receipt_id",
                "mode",
                "profile_hash",
                "namespace_id",
                "run_id",
                "stage_id",
                "attempt_number",
                "content_hash",
                "evidence_hash",
                "receipt_json",
            },
            "daily_pipeline_effect_intent": {
                "run_id",
                "mode",
                "profile_hash",
                "namespace_id",
                "stage_id",
                "attempt_number",
                "effect_id",
                "idempotency_key",
                "adapter_identity",
                "receipt_locator",
                "intent_json",
                "prepared_at",
            },
            "daily_pipeline_schema_migration": {
                "migration_id",
                "from_version",
                "to_version",
                "applied_at",
            },
        }
        rows = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        tables = {str(row["name"]) for row in rows}
        if not set(required_columns) <= tables:
            raise DailyPipelineLedgerError("daily pipeline ledger schema is corrupt")
        for table_name, expected in required_columns.items():
            columns = {
                str(row["name"])
                for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
            }
            if not expected <= columns:
                raise DailyPipelineLedgerError("daily pipeline ledger schema is corrupt")

    def _ensure_receipt_integrity_columns(self, connection: sqlite3.Connection) -> None:
        present = connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name = 'daily_pipeline_stage_receipt'"
        ).fetchone()
        if present is None:
            return
        columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(daily_pipeline_stage_receipt)"
            ).fetchall()
        }
        if "content_hash" not in columns:
            connection.execute(
                "ALTER TABLE daily_pipeline_stage_receipt ADD COLUMN content_hash TEXT"
            )
        if "evidence_hash" not in columns:
            connection.execute(
                "ALTER TABLE daily_pipeline_stage_receipt ADD COLUMN evidence_hash TEXT"
            )
        pending = connection.execute(
            """
            SELECT receipt.receipt_id, receipt.run_id, receipt.stage_id,
                   receipt.attempt_number, receipt.receipt_json, run.input_identity
            FROM daily_pipeline_stage_receipt AS receipt
            JOIN daily_pipeline_run AS run ON run.run_id = receipt.run_id
            WHERE receipt.content_hash IS NULL OR receipt.evidence_hash IS NULL
            """
        ).fetchall()
        for row in pending:
            receipt = _receipt_from_json(row["receipt_json"])
            if (
                receipt.receipt_id != row["receipt_id"]
                or receipt.run_id != row["run_id"]
                or receipt.stage_id != row["stage_id"]
                or receipt.attempt_number != int(row["attempt_number"])
                or receipt.input_identity != row["input_identity"]
            ):
                raise DailyPipelineLedgerError("daily stage receipt binding is corrupt")
            connection.execute(
                """
                UPDATE daily_pipeline_stage_receipt
                SET content_hash = ?, evidence_hash = ?
                WHERE receipt_id = ?
                """,
                (
                    receipt.result.content_hash,
                    receipt.result.evidence_hash,
                    row["receipt_id"],
                ),
            )

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection, binding = self._connect()
        try:
            binding.assert_current()
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            binding.assert_current()
            connection.commit()
            binding.assert_current()
        except _CommittedLedgerError:
            binding.assert_current()
            connection.commit()
            binding.assert_current()
            raise
        except DailyPipelineLedgerError:
            connection.rollback()
            raise
        except sqlite3.Error as exc:
            connection.rollback()
            raise DailyPipelineLedgerError("daily pipeline ledger transaction failed") from exc
        finally:
            connection.close()
            binding.close()

    @contextmanager
    def _snapshot(self) -> Iterator[sqlite3.Connection]:
        connection, binding = self._connect()
        try:
            binding.assert_current()
            yield connection
            binding.assert_current()
        except sqlite3.Error as exc:
            raise DailyPipelineLedgerError("daily pipeline ledger read failed") from exc
        finally:
            connection.close()
            binding.close()

    def acquire_writer(
        self,
        *,
        owner: str,
        now: datetime,
        lease_for: timedelta,
    ) -> DailyWriterLease:
        if owner != self.service_owner:
            raise DailyPipelineLedgerError("writer owner does not match stable service owner")
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        observed = normalize_aware_utc(now)
        expires = observed + lease_for
        with self._transaction() as connection:
            self._validate_schema(connection)
            row = connection.execute(
                "SELECT * FROM daily_pipeline_writer WHERE singleton = 1"
            ).fetchone()
            if row is None:
                raise DailyPipelineLedgerError("daily pipeline writer authority is missing")
            token = int(row["fencing_token"]) + 1
            connection.execute(
                "UPDATE daily_pipeline_writer SET fencing_token = ?, acquired_at = ?, "
                "lease_expires_at = ? WHERE singleton = 1",
                (token, _dump_datetime(observed), _dump_datetime(expires)),
            )
            return DailyWriterLease(
                service_owner=self.service_owner,
                fencing_token=token,
                acquired_at=observed,
                expires_at=expires,
            )

    @contextmanager
    def hold_stage_fence(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        *,
        checked_at: datetime,
    ) -> Iterator[DailyLedgerStageFence]:
        """Hold a claimed stage's lease, source, and input identity through one boundary."""
        verified_lease = DailyWriterLease.model_validate(lease)
        verified_attempt = DailyStageAttempt.model_validate(attempt)
        observed = normalize_aware_utc(checked_at)
        with self._transaction() as connection:
            fence = DailyLedgerStageFence(
                ledger=self,
                connection=connection,
                lease=verified_lease,
                attempt=verified_attempt,
            )
            fence.assert_current(observed)
            yield fence

    @contextmanager
    def hold_external_effect_fence(
        self,
        *,
        run_id: str,
        stage_id: str,
        idempotency_key: str,
        effect_id: str,
        command_manifest_hash: str,
        fencing_token: int,
        checked_at: datetime,
    ) -> Iterator[DailyExternalEffectFence]:
        """Hold the writer transaction while a child commits its signed receipt.

        The child calls this immediately before publication.  ``BEGIN
        IMMEDIATE`` prevents a replacement writer from advancing the fence
        until the immutable receipt has either committed or failed.
        """
        observed = normalize_aware_utc(checked_at)
        with self._transaction() as connection:
            self._validate_schema(connection)
            writer = connection.execute(
                "SELECT * FROM daily_pipeline_writer WHERE singleton = 1"
            ).fetchone()
            if writer is None:
                raise DailyPipelineLedgerError("daily pipeline writer authority is missing")
            writer_expiry = _load_datetime(writer["lease_expires_at"])
            if (
                int(writer["fencing_token"]) != fencing_token
                or writer_expiry is None
                or writer_expiry <= observed
            ):
                raise LeaseLost("external receipt writer lease is stale")
            run = self._run_from_row(self._run_row(connection, run_id))
            if run.state is not DailyRunState.RUNNING:
                raise DailyPipelineLedgerError("external receipt run is not active")
            stage = self._stage_row(connection, run_id, stage_id)
            stage_expiry = _load_datetime(stage["claim_lease_expires_at"])
            if (
                stage["state"] != DailyStageState.RUNNING.value
                or stage["claim_fencing_token"] != fencing_token
                or stage_expiry != writer_expiry
                or stage_expiry is None
                or stage_expiry <= observed
            ):
                raise LeaseLost("external receipt stage fence is stale")
            effect = self._effect_for_stage(connection, run_id, stage_id)
            if effect is None:
                raise DailyPipelineLedgerError("external receipt effect is not prepared")
            if (
                effect.intent.idempotency_key != idempotency_key
                or effect.intent.effect_id != effect_id
                or effect.intent.command_manifest_hash != command_manifest_hash
                or run.spec.command_manifest_hash != command_manifest_hash
            ):
                raise DailyPipelineLedgerError("external receipt effect identity is stale")
            yield DailyExternalEffectFence(
                run_id=run_id,
                mode=run.spec.mode,
                stage_id=stage_id,
                idempotency_key=idempotency_key,
                fencing_token=fencing_token,
                lease_expiry=writer_expiry,
                source_generation_id=run.spec.source_generation_id,
                source_content_hash=run.spec.source_content_hash,
                command_manifest_hash=command_manifest_hash,
                effect_id=effect_id,
            )

    def create_run(
        self,
        lease: DailyWriterLease,
        spec: DailyRunSpec,
        *,
        now: datetime,
    ) -> DailyRunRecord:
        verified = DailyRunSpec.model_validate(spec)
        if (
            verified.mode is not self.storage_profile.mode
            or verified.profile_hash != self.storage_profile.profile_hash
        ):
            raise DailyPipelineLedgerError("daily run does not match the ledger storage profile")
        observed = normalize_aware_utc(now)
        spec_json = _canonical_model_json(verified)
        spec_hash = canonical_sha256(verified)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            existing = connection.execute(
                "SELECT * FROM daily_pipeline_run WHERE run_id = ?", (verified.run_id,)
            ).fetchone()
            if existing is not None:
                if existing["spec_hash"] != spec_hash:
                    raise DailyPipelineLedgerError(
                        "daily run conflicts with immutable input identity"
                    )
                return self._run_from_row(existing)
            connection.execute(
                """
                INSERT INTO daily_pipeline_run(
                    run_id, mode, profile_hash, namespace_id, spec_json, spec_hash,
                    input_identity, state, created_at, cancelled_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    verified.run_id,
                    verified.mode.value,
                    verified.profile_hash,
                    self.storage_profile.namespace_id,
                    spec_json,
                    spec_hash,
                    verified.input_identity,
                    DailyRunState.RUNNING.value,
                    _dump_datetime(observed),
                ),
            )
            for sequence, stage in enumerate(verified.stages):
                connection.execute(
                    """
                    INSERT INTO daily_pipeline_stage(
                        run_id, stage_id, sequence, state, attempts, max_attempts,
                        retry_backoff_seconds, deadline_at
                    ) VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                    """,
                    (
                        verified.run_id,
                        stage.stage_id,
                        sequence,
                        DailyStageState.PENDING.value,
                        stage.max_attempts,
                        stage.retry_backoff_seconds,
                        _dump_datetime(stage.deadline_at),
                    ),
                )
                connection.executemany(
                    "INSERT INTO daily_pipeline_dependency("
                    "run_id, stage_id, dependency_stage_id"
                    ") VALUES (?, ?, ?)",
                    [
                        (verified.run_id, stage.stage_id, dependency)
                        for dependency in stage.depends_on
                    ],
                )
            return DailyRunRecord(
                run_id=verified.run_id,
                spec=verified,
                input_identity=verified.input_identity,
                state=DailyRunState.RUNNING,
                created_at=observed,
            )

    def claim_next(
        self,
        lease: DailyWriterLease,
        *,
        now: datetime,
    ) -> DailyStageAttempt | None:
        return self._claim_next(lease, now=now, run_id=None)

    def claim_next_for_run(
        self,
        lease: DailyWriterLease,
        run_id: str,
        *,
        now: datetime,
    ) -> DailyStageAttempt | None:
        """Atomically claim the next ready stage for one explicit immutable run.

        Consumers must use this authority API instead of scanning the SQLite
        ledger themselves.  It prevents a run-addressed orchestrator from
        accidentally leasing work that belongs to an older run.
        """
        return self._claim_next(lease, now=now, run_id=run_id)

    def _claim_next(
        self,
        lease: DailyWriterLease,
        *,
        now: datetime,
        run_id: str | None,
    ) -> DailyStageAttempt | None:
        observed = normalize_aware_utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            self._expire_deadlines(connection, observed, limit=_DEFAULT_DEADLINE_SWEEP_LIMIT)
            run_predicate = "" if run_id is None else " AND stage.run_id = ?"
            parameters: tuple[object, ...] = (
                DailyStageState.PENDING.value,
                DailyStageState.RETRY_WAIT.value,
                DailyRunState.RUNNING.value,
                _dump_datetime(observed),
                *((run_id,) if run_id is not None else ()),
            )
            for row in connection.execute(
                f"""
                SELECT stage.*, run.spec_json, run.mode AS run_mode,
                       run.state AS run_state
                FROM daily_pipeline_stage AS stage
                JOIN daily_pipeline_run AS run ON run.run_id = stage.run_id
                WHERE stage.state IN (?, ?)
                  AND run.state = ?
                  AND (stage.next_attempt_at IS NULL OR stage.next_attempt_at <= ?)
                  {run_predicate}
                ORDER BY run.created_at, stage.sequence
                """,
                parameters,
            ):
                deadline = self._effective_deadline(row, row["spec_json"])
                if deadline is not None and observed >= deadline:
                    self._mark_failed(
                        connection,
                        row,
                        StageFailure(
                            error_code="deadline_expired",
                            message="stage deadline expired",
                        ),
                    )
                    self._refresh_run_state(connection, row["run_id"])
                    continue
                if not self._dependencies_succeeded(connection, row["run_id"], row["stage_id"]):
                    continue
                if int(row["attempts"]) >= int(row["max_attempts"]):
                    self._mark_failed(
                        connection,
                        row,
                        StageFailure(
                            error_code="attempts_exhausted",
                            message="stage attempts exhausted",
                        ),
                    )
                    self._refresh_run_state(connection, row["run_id"])
                    continue
                attempt_number = int(row["attempts"]) + 1
                connection.execute(
                    """
                    UPDATE daily_pipeline_stage
                    SET state = ?, attempts = ?, next_attempt_at = NULL, claimed_at = ?,
                        claim_fencing_token = ?, claim_lease_expires_at = ?,
                        failure_code = NULL, failure_message = NULL
                    WHERE run_id = ? AND stage_id = ?
                    """,
                    (
                        DailyStageState.RUNNING.value,
                        attempt_number,
                        _dump_datetime(observed),
                        lease.fencing_token,
                        _dump_datetime(lease.expires_at),
                        row["run_id"],
                        row["stage_id"],
                    ),
                )
                return DailyStageAttempt(
                    mode=DailyPipelineMode(row["run_mode"]),
                    run_id=row["run_id"],
                    stage_id=row["stage_id"],
                    attempt_number=attempt_number,
                    fencing_token=lease.fencing_token,
                    claimed_at=observed,
                    lease_expires_at=lease.expires_at,
                )
            return None

    def prepare_effect(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        intent: DailyStageEffectIntent,
        *,
        now: datetime,
    ) -> DailyStageEffectRecord:
        """Durably reserve a deterministic idempotent external operation.

        This record is written before a child process is started.  A later
        process may adopt the same stage and reconcile its immutable external
        receipt without running the side effect a second time.
        """
        verified_attempt = DailyStageAttempt.model_validate(attempt)
        verified_intent = DailyStageEffectIntent.model_validate(intent)
        observed = normalize_aware_utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            row = self._stage_row(connection, verified_attempt.run_id, verified_attempt.stage_id)
            self._assert_active_attempt(row, verified_attempt, lease)
            run = self._run_from_row(self._run_row(connection, verified_attempt.run_id))
            if verified_intent.command_manifest_hash != run.spec.command_manifest_hash:
                raise DailyPipelineLedgerError("daily stage effect command manifest is stale")
            if verified_intent.mode is not run.spec.mode:
                raise DailyPipelineLedgerError("daily stage effect mode is stale")
            existing = self._effect_for_stage(
                connection, verified_attempt.run_id, verified_attempt.stage_id
            )
            if existing is not None:
                if existing.intent != verified_intent:
                    raise DailyPipelineLedgerError(
                        "daily stage effect intent conflicts with durable intent"
                    )
                return existing
            connection.execute(
                """
                INSERT INTO daily_pipeline_effect_intent(
                    run_id, mode, profile_hash, namespace_id, stage_id, attempt_number,
                    effect_id, idempotency_key, adapter_identity, receipt_locator,
                    intent_json, prepared_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    verified_attempt.run_id,
                    run.spec.mode.value,
                    run.spec.profile_hash,
                    self.storage_profile.namespace_id,
                    verified_attempt.stage_id,
                    verified_attempt.attempt_number,
                    verified_intent.effect_id,
                    verified_intent.idempotency_key,
                    verified_intent.adapter_identity,
                    verified_intent.receipt_locator,
                    _canonical_model_json(verified_intent),
                    _dump_datetime(observed),
                ),
            )
            return DailyStageEffectRecord(
                run_id=verified_attempt.run_id,
                mode=run.spec.mode,
                stage_id=verified_attempt.stage_id,
                attempt_number=verified_attempt.attempt_number,
                intent=verified_intent,
                prepared_at=observed,
            )

    def effect_for_stage(
        self,
        run_id: str,
        stage_id: str,
    ) -> DailyStageEffectRecord | None:
        with self._snapshot() as connection:
            self._validate_schema(connection)
            return self._effect_for_stage(connection, run_id, stage_id)

    def adopt_effect_attempt(
        self,
        lease: DailyWriterLease,
        *,
        run_id: str,
        stage_id: str,
        now: datetime,
    ) -> DailyStageAttempt | None:
        """Fence and resume a running stage without incrementing its attempt.

        Adoption does not increment the attempt number.  The same idempotency
        key remains authoritative across a crash; stages without an effect
        remain on their same attempt so the new owner can prepare once.
        """
        observed = normalize_aware_utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            row = self._stage_row(connection, run_id, stage_id)
            if row["state"] != DailyStageState.RUNNING.value:
                return None
            run = self._run_row(connection, run_id)
            self._raise_if_deadline_expired(connection, row, run, observed)
            connection.execute(
                """
                UPDATE daily_pipeline_stage
                SET claimed_at = ?, claim_fencing_token = ?, claim_lease_expires_at = ?
                WHERE run_id = ? AND stage_id = ?
                """,
                (
                    _dump_datetime(observed),
                    lease.fencing_token,
                    _dump_datetime(lease.expires_at),
                    run_id,
                    stage_id,
                ),
            )
            return DailyStageAttempt(
                mode=self._run_from_row(run).spec.mode,
                run_id=run_id,
                stage_id=stage_id,
                attempt_number=int(row["attempts"]),
                fencing_token=lease.fencing_token,
                claimed_at=observed,
                lease_expires_at=lease.expires_at,
            )

    def active_effect_attempts(
        self,
        lease: DailyWriterLease,
        *,
        now: datetime,
        run_id: str | None = None,
        limit: int = _DEFAULT_RECOVERY_LIMIT,
    ) -> tuple[DailyStageAttempt, ...]:
        """Return bounded recoverable external effects through the ledger API."""
        observed = normalize_aware_utc(now)
        bounded_limit = self._validate_recovery_limit(limit)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            predicate = "" if run_id is None else " AND stage.run_id = ?"
            rows = connection.execute(
                f"""
                SELECT stage.*, run.mode AS run_mode
                FROM daily_pipeline_stage AS stage
                JOIN daily_pipeline_effect_intent AS effect
                  ON effect.run_id = stage.run_id AND effect.stage_id = stage.stage_id
                JOIN daily_pipeline_run AS run ON run.run_id = stage.run_id
                WHERE stage.state = ? AND run.state = ?{predicate}
                ORDER BY run.created_at, stage.sequence
                LIMIT ?
                """,
                (
                    DailyStageState.RUNNING.value,
                    DailyRunState.RUNNING.value,
                    *((run_id,) if run_id is not None else ()),
                    bounded_limit,
                ),
            ).fetchall()
            return tuple(
                DailyStageAttempt(
                    mode=DailyPipelineMode(row["run_mode"]),
                    run_id=row["run_id"],
                    stage_id=row["stage_id"],
                    attempt_number=int(row["attempts"]),
                    fencing_token=int(row["claim_fencing_token"]),
                    claimed_at=_load_datetime_required(row["claimed_at"]),
                    lease_expires_at=_load_datetime_required(row["claim_lease_expires_at"]),
                )
                for row in rows
            )

    def active_running_attempts(
        self,
        lease: DailyWriterLease,
        *,
        now: datetime,
        run_id: str | None = None,
        limit: int = _DEFAULT_RECOVERY_LIMIT,
    ) -> tuple[DailyStageAttempt, ...]:
        """Return bounded active attempts that a replacement writer may adopt."""
        observed = normalize_aware_utc(now)
        bounded_limit = self._validate_recovery_limit(limit)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            predicate = "" if run_id is None else " AND stage.run_id = ?"
            rows = connection.execute(
                f"""
                SELECT stage.*, run.mode AS run_mode
                FROM daily_pipeline_stage AS stage
                JOIN daily_pipeline_run AS run ON run.run_id = stage.run_id
                WHERE stage.state = ? AND run.state = ?{predicate}
                ORDER BY run.created_at, stage.sequence
                LIMIT ?
                """,
                (
                    DailyStageState.RUNNING.value,
                    DailyRunState.RUNNING.value,
                    *((run_id,) if run_id is not None else ()),
                    bounded_limit,
                ),
            ).fetchall()
            return tuple(
                DailyStageAttempt(
                    mode=DailyPipelineMode(row["run_mode"]),
                    run_id=row["run_id"],
                    stage_id=row["stage_id"],
                    attempt_number=int(row["attempts"]),
                    fencing_token=int(row["claim_fencing_token"]),
                    claimed_at=_load_datetime_required(row["claimed_at"]),
                    lease_expires_at=_load_datetime_required(row["claim_lease_expires_at"]),
                )
                for row in rows
            )

    def heartbeat_stage(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        *,
        now: datetime,
        lease_for: timedelta,
    ) -> DailyWriterLease:
        """Renew the writer and the active stage before every child boundary."""
        if lease_for <= timedelta(0):
            raise ValueError("lease_for must be positive")
        observed = normalize_aware_utc(now)
        verified_attempt = DailyStageAttempt.model_validate(attempt)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            row = self._stage_row(connection, verified_attempt.run_id, verified_attempt.stage_id)
            self._assert_active_attempt(row, verified_attempt, lease)
            expires = observed + lease_for
            connection.execute(
                """
                UPDATE daily_pipeline_writer
                SET acquired_at = ?, lease_expires_at = ?
                WHERE singleton = 1 AND fencing_token = ?
                """,
                (_dump_datetime(observed), _dump_datetime(expires), lease.fencing_token),
            )
            connection.execute(
                """
                UPDATE daily_pipeline_stage
                SET claim_lease_expires_at = ?
                WHERE run_id = ? AND stage_id = ? AND claim_fencing_token = ?
                """,
                (
                    _dump_datetime(expires),
                    verified_attempt.run_id,
                    verified_attempt.stage_id,
                    lease.fencing_token,
                ),
            )
            return DailyWriterLease(
                service_owner=self.service_owner,
                fencing_token=lease.fencing_token,
                acquired_at=observed,
                expires_at=expires,
            )

    def prepare_success(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        result: StageResult,
        *,
        now: datetime,
    ) -> DailyStageReceipt:
        verified_attempt = DailyStageAttempt.model_validate(attempt)
        return self.prepare_stage_success(
            lease,
            run_id=verified_attempt.run_id,
            stage_id=verified_attempt.stage_id,
            attempt_number=verified_attempt.attempt_number,
            result=result,
            now=now,
            expected_fencing_token=verified_attempt.fencing_token,
        )

    def prepare_stage_success(
        self,
        lease: DailyWriterLease,
        *,
        run_id: str,
        stage_id: str,
        attempt_number: int,
        result: StageResult,
        now: datetime,
        expected_fencing_token: int | None = None,
    ) -> DailyStageReceipt:
        observed = normalize_aware_utc(now)
        verified_result = StageResult.model_validate(result)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            row = self._stage_row(connection, run_id, stage_id)
            existing = self._receipt_for_attempt(connection, run_id, stage_id, attempt_number)
            if existing is not None:
                if existing.result != verified_result:
                    raise DailyPipelineLedgerError("conflicting terminal receipt")
                return existing
            if row["state"] != DailyStageState.RUNNING.value:
                raise DailyPipelineLedgerError("daily stage is not claimed")
            if int(row["attempts"]) != attempt_number:
                raise DailyPipelineLedgerError("daily stage attempt is stale")
            if row["claim_fencing_token"] != lease.fencing_token or (
                expected_fencing_token is not None and expected_fencing_token != lease.fencing_token
            ):
                raise LeaseLost("daily stage fencing token is stale")
            if not self._dependencies_succeeded(connection, run_id, stage_id):
                raise DailyPipelineLedgerError("daily stage dependencies are not complete")
            run = self._run_row(connection, run_id)
            self._raise_if_deadline_expired(connection, row, run, observed)
            receipt = DailyStageReceipt(
                mode=DailyPipelineMode(run["mode"]),
                run_id=run_id,
                stage_id=stage_id,
                attempt_number=attempt_number,
                input_identity=run["input_identity"],
                result=verified_result,
                prepared_at=observed,
            )
            connection.execute(
                """
                INSERT INTO daily_pipeline_stage_receipt(
                    receipt_id, mode, profile_hash, namespace_id, run_id, stage_id,
                    attempt_number, content_hash, evidence_hash, receipt_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt.receipt_id,
                    receipt.mode.value,
                    self.storage_profile.profile_hash,
                    self.storage_profile.namespace_id,
                    run_id,
                    stage_id,
                    attempt_number,
                    receipt.result.content_hash,
                    receipt.result.evidence_hash,
                    _canonical_model_json(receipt),
                ),
            )
            return receipt

    def finalize_success(
        self,
        lease: DailyWriterLease,
        receipt: DailyStageReceipt,
        *,
        now: datetime,
    ) -> DailyStageReceipt:
        verified = DailyStageReceipt.model_validate(receipt)
        observed = normalize_aware_utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            row = self._stage_row(connection, verified.run_id, verified.stage_id)
            stored = self._receipt_for_attempt(
                connection, verified.run_id, verified.stage_id, verified.attempt_number
            )
            if stored != verified:
                raise DailyPipelineLedgerError("daily stage receipt is missing or corrupt")
            if row["state"] == DailyStageState.SUCCEEDED.value:
                terminal = self._terminal_receipt_for_stage(connection, row)
                if terminal != verified:
                    raise DailyPipelineLedgerError(
                        "daily stage terminal receipt binding is corrupt"
                    )
                return verified
            if row["state"] != DailyStageState.RUNNING.value:
                raise DailyPipelineLedgerError("daily stage cannot transition to success")
            if int(row["attempts"]) != verified.attempt_number:
                raise DailyPipelineLedgerError("daily stage receipt attempt is stale")
            if row["claim_fencing_token"] != lease.fencing_token:
                raise LeaseLost("daily stage fencing token is stale")
            if not self._dependencies_succeeded(connection, verified.run_id, verified.stage_id):
                raise DailyPipelineLedgerError("daily stage dependencies are not complete")
            run = self._run_row(connection, verified.run_id)
            deadline = self._effective_deadline(row, run["spec_json"])
            if deadline is not None and observed >= deadline:
                self._mark_failed(
                    connection,
                    row,
                    StageFailure(
                        error_code="deadline_expired",
                        message="stage deadline expired",
                    ),
                    terminal_receipt_id=verified.receipt_id,
                )
                self._refresh_run_state(connection, verified.run_id)
                raise _CommittedLedgerError("daily stage deadline expired")
            self._mark_succeeded(connection, row, verified.receipt_id)
            self._refresh_run_state(connection, verified.run_id)
            return verified

    def succeed(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        result: StageResult,
        *,
        now: datetime,
    ) -> DailyStageReceipt:
        receipt = self.prepare_success(lease, attempt, result, now=now)
        return self.finalize_success(lease, receipt, now=now)

    def fail(
        self,
        lease: DailyWriterLease,
        attempt: DailyStageAttempt,
        failure: StageFailure,
        *,
        retryable: bool,
        now: datetime,
    ) -> DailyStageRecord:
        verified_attempt = DailyStageAttempt.model_validate(attempt)
        verified_failure = StageFailure.model_validate(failure)
        observed = normalize_aware_utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            row = self._stage_row(connection, verified_attempt.run_id, verified_attempt.stage_id)
            if (
                self._receipt_for_attempt(
                    connection,
                    verified_attempt.run_id,
                    verified_attempt.stage_id,
                    verified_attempt.attempt_number,
                )
                is not None
            ):
                raise DailyPipelineLedgerError("stage has a prepared terminal receipt")
            self._assert_active_attempt(row, verified_attempt, lease)
            run = self._run_row(connection, verified_attempt.run_id)
            deadline = self._effective_deadline(row, run["spec_json"])
            if deadline is not None and observed >= deadline:
                self._mark_failed(
                    connection,
                    row,
                    StageFailure(
                        error_code="deadline_expired",
                        message="stage deadline expired",
                    ),
                )
                self._refresh_run_state(connection, verified_attempt.run_id)
                return self._stage_from_row(
                    self._stage_row(connection, verified_attempt.run_id, verified_attempt.stage_id)
                )
            exhausted = int(row["attempts"]) >= int(row["max_attempts"])
            if retryable and not exhausted:
                delay = int(row["retry_backoff_seconds"]) * (2 ** (int(row["attempts"]) - 1))
                next_attempt = observed + timedelta(seconds=min(delay, _MAX_BACKOFF_SECONDS))
                connection.execute(
                    """
                    UPDATE daily_pipeline_stage
                    SET state = ?, next_attempt_at = ?, claimed_at = NULL,
                        claim_fencing_token = NULL, claim_lease_expires_at = NULL,
                        failure_code = ?, failure_message = ?
                    WHERE run_id = ? AND stage_id = ?
                    """,
                    (
                        DailyStageState.RETRY_WAIT.value,
                        _dump_datetime(next_attempt),
                        verified_failure.error_code,
                        verified_failure.message,
                        verified_attempt.run_id,
                        verified_attempt.stage_id,
                    ),
                )
            else:
                self._mark_failed(connection, row, verified_failure)
                self._refresh_run_state(connection, verified_attempt.run_id)
            return self._stage_from_row(
                self._stage_row(connection, verified_attempt.run_id, verified_attempt.stage_id)
            )

    def cancel_run(
        self,
        lease: DailyWriterLease,
        run_id: str,
        *,
        reason: str,
        now: datetime,
    ) -> DailyRunRecord:
        if not reason or len(reason) > _MAX_ERROR_MESSAGE:
            raise ValueError("cancel reason is invalid")
        observed = normalize_aware_utc(now)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            run = self._run_row(connection, run_id)
            self._validate_terminal_receipts_for_run(connection, run_id)
            if run["state"] == DailyRunState.SUCCEEDED.value:
                raise DailyPipelineLedgerError("completed daily run cannot be cancelled")
            for row in connection.execute(
                """
                SELECT *
                FROM daily_pipeline_stage
                WHERE run_id = ?
                  AND state NOT IN (?, ?, ?)
                ORDER BY sequence
                """,
                (
                    run_id,
                    DailyStageState.SUCCEEDED.value,
                    DailyStageState.FAILED.value,
                    DailyStageState.CANCELLED.value,
                ),
            ).fetchall():
                prepared = None
                if row["state"] == DailyStageState.RUNNING.value:
                    prepared = self._receipt_for_attempt(
                        connection, run_id, row["stage_id"], int(row["attempts"])
                    )
                if prepared is not None:
                    if not self._dependencies_succeeded(connection, run_id, row["stage_id"]):
                        self._mark_failed(
                            connection,
                            row,
                            StageFailure(
                                error_code="future_stage_receipt",
                                message="prepared receipt has incomplete dependencies",
                            ),
                            terminal_receipt_id=prepared.receipt_id,
                        )
                        continue
                    deadline = self._effective_deadline(row, run["spec_json"])
                    if deadline is not None and observed >= deadline:
                        self._mark_failed(
                            connection,
                            row,
                            StageFailure(
                                error_code="deadline_expired",
                                message="stage deadline expired",
                            ),
                            terminal_receipt_id=prepared.receipt_id,
                        )
                        continue
                    self._mark_succeeded(connection, row, prepared.receipt_id)
                    continue
                connection.execute(
                    """
                    UPDATE daily_pipeline_stage
                    SET state = ?, terminal_receipt_id = NULL, next_attempt_at = NULL,
                        claimed_at = NULL, claim_fencing_token = NULL,
                        claim_lease_expires_at = NULL, failure_code = ?, failure_message = ?
                    WHERE run_id = ? AND stage_id = ?
                    """,
                    (
                        DailyStageState.CANCELLED.value,
                        "cancelled",
                        reason,
                        run_id,
                        row["stage_id"],
                    ),
                )
            states = {
                DailyStageState(state_row["state"])
                for state_row in connection.execute(
                    "SELECT state FROM daily_pipeline_stage WHERE run_id = ?",
                    (run_id,),
                ).fetchall()
            }
            run_state = DailyRunState.CANCELLED
            cancelled_reason = reason
            if DailyStageState.FAILED in states:
                run_state = DailyRunState.FAILED
                cancelled_reason = None
            elif states == {DailyStageState.SUCCEEDED}:
                run_state = DailyRunState.SUCCEEDED
                cancelled_reason = None
            connection.execute(
                "UPDATE daily_pipeline_run SET state = ?, cancelled_reason = ? WHERE run_id = ?",
                (run_state.value, cancelled_reason, run_id),
            )
            return self._run_from_row(self._run_row(connection, run_id))

    def recover(
        self,
        lease: DailyWriterLease,
        *,
        now: datetime,
        limit: int = _DEFAULT_RECOVERY_LIMIT,
        cursor: str | None = None,
        run_id: str | None = None,
    ) -> DailyRecoverySummary:
        observed = normalize_aware_utc(now)
        finalized: list[Sha256] = []
        retried: list[str] = []
        failed: list[str] = []
        bounded_limit = self._validate_recovery_limit(limit)
        cursor_key = _decode_cursor(cursor)
        with self._transaction() as connection:
            self._validate_lease(connection, lease, observed)
            rows, next_cursor = self._active_stage_page(
                connection,
                states=(
                    DailyStageState.PENDING.value,
                    DailyStageState.RETRY_WAIT.value,
                    DailyStageState.RUNNING.value,
                    DailyStageState.SUCCEEDED.value,
                ),
                cursor=cursor_key,
                limit=bounded_limit,
                run_id=run_id,
            )
            for row in rows:
                state = DailyStageState(row["state"])
                deadline = self._effective_deadline(row, row["spec_json"])
                if (
                    state
                    in {
                        DailyStageState.PENDING,
                        DailyStageState.RETRY_WAIT,
                        DailyStageState.RUNNING,
                    }
                    and deadline is not None
                    and observed >= deadline
                ):
                    prepared = None
                    if state is DailyStageState.RUNNING:
                        prepared = self._receipt_for_attempt(
                            connection,
                            row["run_id"],
                            row["stage_id"],
                            int(row["attempts"]),
                        )
                    self._mark_failed(
                        connection,
                        row,
                        StageFailure(
                            error_code="deadline_expired",
                            message="stage deadline expired",
                        ),
                        terminal_receipt_id=None if prepared is None else prepared.receipt_id,
                    )
                    self._refresh_run_state(connection, row["run_id"])
                    failed.append(row["stage_id"])
                    continue
                if state is DailyStageState.SUCCEEDED:
                    receipt = self._terminal_receipt_for_stage(connection, row)
                    if receipt is None:
                        self._mark_failed(
                            connection,
                            row,
                            StageFailure(
                                error_code="missing_terminal_receipt",
                                message="succeeded stage has no immutable receipt",
                            ),
                        )
                        self._refresh_run_state(connection, row["run_id"])
                        failed.append(row["stage_id"])
                    continue
                if state is not DailyStageState.RUNNING:
                    continue
                prepared = self._receipt_for_attempt(
                    connection,
                    row["run_id"],
                    row["stage_id"],
                    int(row["attempts"]),
                )
                if prepared is not None:
                    if not self._dependencies_succeeded(connection, row["run_id"], row["stage_id"]):
                        self._mark_failed(
                            connection,
                            row,
                            StageFailure(
                                error_code="future_stage_receipt",
                                message="prepared receipt has incomplete dependencies",
                            ),
                            terminal_receipt_id=prepared.receipt_id,
                        )
                        self._refresh_run_state(connection, row["run_id"])
                        failed.append(row["stage_id"])
                        continue
                    self._mark_succeeded(connection, row, prepared.receipt_id)
                    self._refresh_run_state(connection, row["run_id"])
                    finalized.append(prepared.receipt_id)
                    continue
                # A replacement writer adopts every nonterminal attempt before
                # reconciling or preparing its effect.  Recovery must not turn
                # a lease loss into an old-owner terminal or retry mutation.
                continue
        return DailyRecoverySummary(
            finalized_receipt_ids=tuple(finalized),
            retried_stage_ids=tuple(retried),
            failed_stage_ids=tuple(failed),
            next_cursor=next_cursor,
        )

    def stage(self, run_id: str, stage_id: str) -> DailyStageRecord:
        with self._snapshot() as connection:
            self._validate_schema(connection)
            row = self._stage_row(connection, run_id, stage_id)
            self._terminal_receipt_for_stage(connection, row)
            return self._stage_from_row(row)

    def run(self, run_id: str) -> DailyRunRecord:
        with self._snapshot() as connection:
            self._validate_schema(connection)
            row = self._run_row(connection, run_id)
            self._validate_terminal_receipts_for_run(connection, run_id)
            return self._run_from_row(row)

    def receipt(self, receipt_id: str) -> DailyStageReceipt | None:
        with self._snapshot() as connection:
            self._validate_schema(connection)
            return self._receipt_for_terminal_id(connection, receipt_id)

    def _validate_lease(
        self,
        connection: sqlite3.Connection,
        lease: DailyWriterLease,
        now: datetime,
    ) -> None:
        verified = DailyWriterLease.model_validate(lease)
        self._validate_schema(connection)
        if verified.service_owner != self.service_owner:
            raise DailyPipelineLedgerError("writer lease owner is invalid")
        row = connection.execute(
            "SELECT * FROM daily_pipeline_writer WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise DailyPipelineLedgerError("daily pipeline writer authority is missing")
        expires = _load_datetime(row["lease_expires_at"])
        if (
            int(row["fencing_token"]) != verified.fencing_token
            or expires is None
            or expires != normalize_aware_utc(verified.expires_at)
            or expires <= now
        ):
            raise LeaseLost("writer lease is stale")

    @staticmethod
    def _run_row(connection: sqlite3.Connection, run_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM daily_pipeline_run WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise DailyPipelineLedgerError("daily run does not exist")
        return row

    @staticmethod
    def _stage_row(connection: sqlite3.Connection, run_id: str, stage_id: str) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM daily_pipeline_stage WHERE run_id = ? AND stage_id = ?",
            (run_id, stage_id),
        ).fetchone()
        if row is None:
            raise DailyPipelineLedgerError("daily stage does not exist")
        return row

    def _dependencies_succeeded(
        self, connection: sqlite3.Connection, run_id: str, stage_id: str
    ) -> bool:
        dependencies = connection.execute(
            """
            SELECT stage.*
            FROM daily_pipeline_dependency AS dependency
            JOIN daily_pipeline_stage AS stage
              ON stage.run_id = dependency.run_id
             AND stage.stage_id = dependency.dependency_stage_id
            WHERE dependency.run_id = ?
              AND dependency.stage_id = ?
            ORDER BY stage.sequence, stage.stage_id
            """,
            (run_id, stage_id),
        ).fetchall()
        for dependency in dependencies:
            if dependency["state"] != DailyStageState.SUCCEEDED.value:
                return False
            self._terminal_receipt_for_stage(connection, dependency)
        return True

    @staticmethod
    def _effective_deadline(stage_row: sqlite3.Row, spec_json: str) -> datetime | None:
        return _load_datetime(stage_row["deadline_at"]) or _spec_from_json(spec_json).deadline_at

    def _raise_if_deadline_expired(
        self,
        connection: sqlite3.Connection,
        stage_row: sqlite3.Row,
        run_row: sqlite3.Row,
        now: datetime,
    ) -> None:
        deadline = self._effective_deadline(stage_row, run_row["spec_json"])
        if deadline is None or now < deadline:
            return
        self._mark_failed(
            connection,
            stage_row,
            StageFailure(
                error_code="deadline_expired",
                message="stage deadline expired",
            ),
        )
        self._refresh_run_state(connection, stage_row["run_id"])
        raise _CommittedLedgerError("daily stage deadline expired")

    def _receipt_for_attempt(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        stage_id: str,
        attempt_number: int,
    ) -> DailyStageReceipt | None:
        row = connection.execute(
            """
            SELECT receipt.receipt_id, receipt.run_id, receipt.stage_id,
                   receipt.mode, receipt.profile_hash, receipt.namespace_id,
                   receipt.attempt_number, receipt.content_hash, receipt.evidence_hash,
                   receipt.receipt_json, run.input_identity
            FROM daily_pipeline_stage_receipt AS receipt
            JOIN daily_pipeline_run AS run ON run.run_id = receipt.run_id
            WHERE receipt.run_id = ? AND receipt.stage_id = ? AND receipt.attempt_number = ?
            """,
            (run_id, stage_id, attempt_number),
        ).fetchone()
        return None if row is None else self._receipt_from_storage(row)

    def _effect_from_row(self, row: sqlite3.Row) -> DailyStageEffectRecord:
        try:
            intent = DailyStageEffectIntent.model_validate_json(row["intent_json"])
        except (TypeError, ValueError) as exc:
            raise DailyPipelineLedgerError("daily stage effect intent is corrupt") from exc
        if (
            intent.effect_id != row["effect_id"]
            or intent.mode.value != row["mode"]
            or intent.idempotency_key != row["idempotency_key"]
            or intent.adapter_identity != row["adapter_identity"]
            or intent.receipt_locator != row["receipt_locator"]
        ):
            raise DailyPipelineLedgerError("daily stage effect intent binding is corrupt")
        if (
            row["mode"] != self.storage_profile.mode.value
            or row["profile_hash"] != self.storage_profile.profile_hash
            or row["namespace_id"] != self.storage_profile.namespace_id
        ):
            raise DailyPipelineLedgerError("daily stage effect storage profile mismatch")
        return DailyStageEffectRecord(
            run_id=row["run_id"],
            mode=intent.mode,
            stage_id=row["stage_id"],
            attempt_number=int(row["attempt_number"]),
            intent=intent,
            prepared_at=_load_datetime_required(row["prepared_at"]),
        )

    def _effect_for_stage(
        self,
        connection: sqlite3.Connection,
        run_id: str,
        stage_id: str,
    ) -> DailyStageEffectRecord | None:
        row = connection.execute(
            """
            SELECT *
            FROM daily_pipeline_effect_intent
            WHERE run_id = ? AND stage_id = ?
            """,
            (run_id, stage_id),
        ).fetchone()
        return None if row is None else self._effect_from_row(row)

    def _receipt_for_terminal_id(
        self, connection: sqlite3.Connection, receipt_id: str | None
    ) -> DailyStageReceipt | None:
        if receipt_id is None:
            return None
        row = connection.execute(
            """
            SELECT receipt.receipt_id, receipt.run_id, receipt.stage_id,
                   receipt.mode, receipt.profile_hash, receipt.namespace_id,
                   receipt.attempt_number, receipt.content_hash, receipt.evidence_hash,
                   receipt.receipt_json, run.input_identity
            FROM daily_pipeline_stage_receipt AS receipt
            JOIN daily_pipeline_run AS run ON run.run_id = receipt.run_id
            WHERE receipt.receipt_id = ?
            """,
            (receipt_id,),
        ).fetchone()
        return None if row is None else self._receipt_from_storage(row)

    def _terminal_receipt_for_stage(
        self,
        connection: sqlite3.Connection,
        stage_row: sqlite3.Row,
    ) -> DailyStageReceipt | None:
        receipt = self._receipt_for_terminal_id(connection, stage_row["terminal_receipt_id"])
        if receipt is None:
            return None
        run = self._run_row(connection, stage_row["run_id"])
        if (
            receipt.run_id != stage_row["run_id"]
            or receipt.stage_id != stage_row["stage_id"]
            or receipt.attempt_number != int(stage_row["attempts"])
            or receipt.input_identity != run["input_identity"]
        ):
            raise DailyPipelineLedgerError("daily stage terminal receipt binding is corrupt")
        return receipt

    def _validate_terminal_receipts_for_run(
        self,
        connection: sqlite3.Connection,
        run_id: str,
    ) -> None:
        rows = connection.execute(
            """
            SELECT *
            FROM daily_pipeline_stage
            WHERE run_id = ? AND terminal_receipt_id IS NOT NULL
            ORDER BY sequence, stage_id
            """,
            (run_id,),
        ).fetchall()
        for row in rows:
            self._terminal_receipt_for_stage(connection, row)

    def _receipt_from_storage(self, row: sqlite3.Row) -> DailyStageReceipt:
        receipt = _receipt_from_json(row["receipt_json"])
        if (
            receipt.receipt_id != row["receipt_id"]
            or receipt.mode.value != row["mode"]
            or receipt.run_id != row["run_id"]
            or receipt.stage_id != row["stage_id"]
            or receipt.attempt_number != int(row["attempt_number"])
            or receipt.input_identity != row["input_identity"]
            or receipt.result.content_hash != row["content_hash"]
            or receipt.result.evidence_hash != row["evidence_hash"]
        ):
            raise DailyPipelineLedgerError("daily stage receipt binding is corrupt")
        if (
            row["mode"] != self.storage_profile.mode.value
            or row["profile_hash"] != self.storage_profile.profile_hash
            or row["namespace_id"] != self.storage_profile.namespace_id
        ):
            raise DailyPipelineLedgerError("daily stage receipt storage profile mismatch")
        return receipt

    @staticmethod
    def _assert_active_attempt(
        row: sqlite3.Row,
        attempt: DailyStageAttempt,
        lease: DailyWriterLease,
    ) -> None:
        if row["state"] != DailyStageState.RUNNING.value:
            raise DailyPipelineLedgerError("daily stage is not running")
        if int(row["attempts"]) != attempt.attempt_number:
            raise DailyPipelineLedgerError("daily stage attempt is stale")
        if (
            attempt.fencing_token != lease.fencing_token
            or row["claim_fencing_token"] != lease.fencing_token
        ):
            raise LeaseLost("daily stage fencing token is stale")

    @staticmethod
    def _validate_recovery_limit(limit: int) -> int:
        if limit < 1 or limit > _MAX_RECOVERY_LIMIT:
            raise ValueError(f"recovery limit must be between 1 and {_MAX_RECOVERY_LIMIT}")
        return limit

    def _active_stage_page(
        self,
        connection: sqlite3.Connection,
        *,
        states: tuple[str, ...],
        cursor: tuple[str, int, str] | None,
        limit: int,
        run_id: str | None = None,
    ) -> tuple[list[sqlite3.Row], str | None]:
        placeholders = ", ".join("?" for _ in states)
        query = f"""
            SELECT stage.*, run.spec_json, run.state AS run_state
            FROM daily_pipeline_stage AS stage
            JOIN daily_pipeline_run AS run ON run.run_id = stage.run_id
            WHERE run.state = ?
              AND stage.state IN ({placeholders})
        """
        params: list[object] = [DailyRunState.RUNNING.value, *states]
        if run_id is not None:
            query += " AND stage.run_id = ?"
            params.append(run_id)
        if cursor is not None:
            run_id, sequence, stage_id = cursor
            query += """
              AND (
                    stage.run_id > ?
                 OR (stage.run_id = ? AND stage.sequence > ?)
                 OR (stage.run_id = ? AND stage.sequence = ? AND stage.stage_id > ?)
              )
            """
            params.extend((run_id, run_id, sequence, run_id, sequence, stage_id))
        query += """
            ORDER BY stage.run_id, stage.sequence, stage.stage_id
            LIMIT ?
        """
        params.append(limit + 1)
        rows = connection.execute(query, tuple(params)).fetchall()
        next_cursor = None
        if len(rows) > limit:
            rows.pop()
            marker = rows[-1]
            next_cursor = _encode_cursor(
                str(marker["run_id"]),
                int(marker["sequence"]),
                str(marker["stage_id"]),
            )
        return rows, next_cursor

    def _expire_deadlines(
        self,
        connection: sqlite3.Connection,
        now: datetime,
        *,
        limit: int = _DEFAULT_DEADLINE_SWEEP_LIMIT,
    ) -> None:
        bounded_limit = self._validate_recovery_limit(limit)
        rows, _ = self._active_stage_page(
            connection,
            states=(
                DailyStageState.PENDING.value,
                DailyStageState.RETRY_WAIT.value,
                DailyStageState.RUNNING.value,
            ),
            cursor=None,
            limit=bounded_limit,
        )
        for row in rows:
            deadline = self._effective_deadline(row, row["spec_json"])
            if deadline is not None and now >= deadline:
                prepared = None
                if row["state"] == DailyStageState.RUNNING.value:
                    prepared = self._receipt_for_attempt(
                        connection,
                        row["run_id"],
                        row["stage_id"],
                        int(row["attempts"]),
                    )
                self._mark_failed(
                    connection,
                    row,
                    StageFailure(error_code="deadline_expired", message="stage deadline expired"),
                    terminal_receipt_id=None if prepared is None else prepared.receipt_id,
                )
                self._refresh_run_state(connection, row["run_id"])

    @staticmethod
    def _mark_failed(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        failure: StageFailure,
        *,
        terminal_receipt_id: str | None = None,
    ) -> None:
        connection.execute(
            """
            UPDATE daily_pipeline_stage
            SET state = ?, terminal_receipt_id = ?, next_attempt_at = NULL, claimed_at = NULL,
                claim_fencing_token = NULL, claim_lease_expires_at = NULL,
                failure_code = ?, failure_message = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (
                DailyStageState.FAILED.value,
                terminal_receipt_id,
                failure.error_code,
                failure.message,
                row["run_id"],
                row["stage_id"],
            ),
        )

    @staticmethod
    def _mark_succeeded(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        receipt_id: str,
    ) -> None:
        connection.execute(
            """
            UPDATE daily_pipeline_stage
            SET state = ?, terminal_receipt_id = ?, next_attempt_at = NULL,
                claimed_at = NULL, claim_fencing_token = NULL,
                claim_lease_expires_at = NULL, failure_code = NULL, failure_message = NULL
            WHERE run_id = ? AND stage_id = ?
            """,
            (
                DailyStageState.SUCCEEDED.value,
                receipt_id,
                row["run_id"],
                row["stage_id"],
            ),
        )

    def _refresh_run_state(self, connection: sqlite3.Connection, run_id: str) -> None:
        run = self._run_row(connection, run_id)
        if run["state"] == DailyRunState.CANCELLED.value:
            return
        self._validate_terminal_receipts_for_run(connection, run_id)
        states = {
            DailyStageState(row["state"])
            for row in connection.execute(
                "SELECT state FROM daily_pipeline_stage WHERE run_id = ?", (run_id,)
            ).fetchall()
        }
        state = DailyRunState.RUNNING
        if DailyStageState.FAILED in states:
            state = DailyRunState.FAILED
        elif states == {DailyStageState.SUCCEEDED}:
            state = DailyRunState.SUCCEEDED
        connection.execute(
            "UPDATE daily_pipeline_run SET state = ? WHERE run_id = ?", (state.value, run_id)
        )

    @staticmethod
    def _stage_from_row(row: sqlite3.Row) -> DailyStageRecord:
        failure = None
        if row["failure_code"] is not None:
            failure = StageFailure(error_code=row["failure_code"], message=row["failure_message"])
        return DailyStageRecord(
            run_id=row["run_id"],
            stage_id=row["stage_id"],
            sequence=int(row["sequence"]),
            state=DailyStageState(row["state"]),
            attempts=int(row["attempts"]),
            next_attempt_at=_load_datetime(row["next_attempt_at"]),
            claimed_at=_load_datetime(row["claimed_at"]),
            claim_fencing_token=row["claim_fencing_token"],
            claim_lease_expires_at=_load_datetime(row["claim_lease_expires_at"]),
            terminal_receipt_id=row["terminal_receipt_id"],
            last_failure=failure,
        )

    def _run_from_row(self, row: sqlite3.Row) -> DailyRunRecord:
        spec = _spec_from_json(row["spec_json"])
        if (
            spec.mode.value != row["mode"]
            or spec.profile_hash != row["profile_hash"]
            or row["mode"] != self.storage_profile.mode.value
            or row["profile_hash"] != self.storage_profile.profile_hash
            or row["namespace_id"] != self.storage_profile.namespace_id
        ):
            raise DailyPipelineLedgerError("daily run storage profile binding is corrupt")
        return DailyRunRecord(
            run_id=row["run_id"],
            spec=spec,
            input_identity=row["input_identity"],
            state=DailyRunState(row["state"]),
            created_at=_load_datetime_required(row["created_at"]),
            cancelled_reason=row["cancelled_reason"],
        )


def _encode_cursor(run_id: str, sequence: int, stage_id: str) -> str:
    return f"{run_id}|{sequence}|{stage_id}"


def _decode_cursor(cursor: str | None) -> tuple[str, int, str] | None:
    if cursor is None:
        return None
    try:
        run_id, sequence_text, stage_id = cursor.split("|", maxsplit=2)
        sequence = int(sequence_text)
    except (AttributeError, TypeError, ValueError) as exc:
        raise DailyPipelineLedgerError("daily recovery cursor is invalid") from exc
    if not run_id or not stage_id or sequence < 0:
        raise DailyPipelineLedgerError("daily recovery cursor is invalid")
    return run_id, sequence, stage_id


def _dump_datetime(value: datetime | None) -> str | None:
    return None if value is None else normalize_aware_utc(value).isoformat(timespec="microseconds")


def _load_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return normalize_aware_utc(parsed)
    except (TypeError, ValueError) as exc:
        raise DailyPipelineLedgerError(
            "daily pipeline ledger contains an invalid datetime"
        ) from exc


def _load_datetime_required(value: str | None) -> datetime:
    parsed = _load_datetime(value)
    if parsed is None:
        raise DailyPipelineLedgerError("daily pipeline ledger requires a datetime")
    return parsed


def _canonical_model_json(model: RuntimeContractModel) -> str:
    return json.dumps(
        model.model_dump(mode="json"),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )


def _spec_from_json(payload: str) -> DailyRunSpec:
    try:
        value = json.loads(payload)
        return DailyRunSpec.model_validate(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DailyPipelineLedgerError("daily pipeline run spec is corrupt") from exc


def _receipt_from_json(payload: str) -> DailyStageReceipt:
    try:
        value = json.loads(payload)
        return DailyStageReceipt.model_validate(value)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise DailyPipelineLedgerError("daily stage receipt is corrupt") from exc
