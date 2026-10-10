"""Private SQLite authority for recoverable, uniquely indexed factor evaluations."""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Lock, local
from typing import Literal
from uuid import uuid4

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    field_validator,
    model_validator,
)

from rquant.factor.display_artifact import (
    FactorDisplayArtifactV1,
    _load_factor_display_artifact_with_identity,
    project_factor_display_artifact,
)
from rquant.factor.job_runner import FactorEvaluationCompletion
from rquant.factor.job_spec import FactorEvaluationJobSpec
from rquant.factor.result_artifact import (
    FactorResearchArtifactV1,
    _load_factor_research_artifact_with_identity,
)
from rquant.factor.stream_job_artifact import StreamArtifactWitness, verify_factor_stream_artifacts
from rquant.factor.stream_job_runner import FactorStreamCompletion, decode_factor_completion_json
from rquant.factor.stream_job_spec import FactorStreamJobSpec, decode_factor_job_spec_json
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_COMMAND_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_HEX32 = re.compile(r"[0-9a-f]{32}\Z")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_SCHEMA_VERSION = 1
_TABLES = frozenset({"factor_ledger_identity", "factor_jobs", "factor_commands"})
_MAX_SPEC_BYTES = 2 * 1024 * 1024
_MAX_COMPLETION_BYTES = 64 * 1024
_MAX_LIST = 200
_MAX_LEASE_SECONDS = 3600

FactorJobStatus = Literal["queued", "running", "succeeded", "failed"]
FactorJobFailureCode = Literal[
    "deadline_expired",
    "source_unavailable",
    "artifact_invalid",
    "evaluation_failed",
    "internal_error",
]


class FactorLedgerError(RuntimeError):
    """Base failure for the independent factor job authority."""


class FactorLedgerIdentityError(FactorLedgerError):
    """The externally pinned ledger file or instance has changed."""


class FactorLedgerIntegrityError(FactorLedgerError):
    """Persistent rows or schema cannot be trusted."""


class FactorLedgerConflictError(FactorLedgerError):
    """A command identity was reused for different job content."""


class FactorLedgerLeaseError(FactorLedgerError):
    """The caller no longer owns the live version of the job."""


class FactorLedgerCompletionError(FactorLedgerError):
    """The sealed result does not prove this exact job."""


class FactorLedgerIdentity(BaseModel):
    """Store this outside the ledger and require it when reopening an existing file."""

    model_config = _IMMUTABLE

    instance_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    path: str
    st_dev: int = Field(ge=0, strict=True)
    st_ino: int = Field(gt=0, strict=True)

    @model_validator(mode="after")
    def _absolute_path(self) -> FactorLedgerIdentity:
        if not self.path.startswith("/") or os.path.normpath(self.path) != self.path:
            raise ValueError("factor ledger identity requires a canonical absolute path")
        return self


class FactorJobRecord(BaseModel):
    """Bounded public state; no lease token or arbitrary storage path escapes."""

    model_config = _IMMUTABLE

    job_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    spec: FactorEvaluationJobSpec | FactorStreamJobSpec
    spec_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: FactorJobStatus
    version: int = Field(ge=0, strict=True)
    attempts: int = Field(ge=0, strict=True)
    created_at: AwareDatetime
    updated_at: AwareDatetime
    lease_expires_at: AwareDatetime | None
    failure_code: FactorJobFailureCode | None
    completion: FactorEvaluationCompletion | FactorStreamCompletion | None


class FactorJobLease(BaseModel):
    """One versioned worker capability; a new claim always rotates its token."""

    model_config = _IMMUTABLE

    job: FactorJobRecord
    lease_token: str = Field(pattern=r"^[0-9a-f]{64}$")
    version: int = Field(ge=1, strict=True)
    expires_at: AwareDatetime


@dataclass(frozen=True, slots=True)
class _VerifiedSpec:
    spec: FactorEvaluationJobSpec | FactorStreamJobSpec


@dataclass(frozen=True, slots=True)
class _ClaimedSpec:
    instance: FactorLedgerIdentity
    job_id: str
    lease_token: str
    version: int
    spec: FactorEvaluationJobSpec | FactorStreamJobSpec


class _JobState(FactorJobRecord):
    lease_token: str | None

    @field_validator("spec", mode="wrap")
    @classmethod
    def _same_verified_spec(
        cls, value: object, handler: ValidatorFunctionWrapHandler, info: ValidationInfo
    ) -> object:
        # Only the exact immutable instance from a complete persisted decode can be reused.
        if isinstance(info.context, _VerifiedSpec) and value is info.context.spec:
            return value
        return handler(value)

    @model_validator(mode="after")
    def _valid_state(self) -> _JobState:
        if self.spec_sha256 != self.spec.spec_sha256 or self.created_at > self.updated_at:
            raise ValueError("factor job spec digest or time range differs")
        if self.status == "queued":
            valid = (
                self.version == 0
                and self.attempts == 0
                and self.lease_token is None
                and self.lease_expires_at is None
                and self.failure_code is None
                and self.completion is None
            )
        elif self.status == "running":
            valid = (
                self.attempts >= 1
                and self.version >= self.attempts
                and isinstance(self.lease_token, str)
                and _HEX64.fullmatch(self.lease_token) is not None
                and self.lease_expires_at is not None
                and self.lease_expires_at > self.updated_at
                and self.lease_expires_at <= self.spec.deadline
                and self.failure_code is None
                and self.completion is None
            )
        elif self.status == "succeeded":
            valid = (
                self.attempts >= 1
                and self.version > self.attempts
                and self.lease_token is None
                and self.lease_expires_at is None
                and self.failure_code is None
                and self.completion is not None
                and self.completion.spec_sha256 == self.spec_sha256
                and self.completion.code_revision == self.spec.code_revision
                and self.completion.completed_at <= self.updated_at
                and isinstance(self.completion, FactorStreamCompletion)
                == isinstance(self.spec, FactorStreamJobSpec)
            )
        else:
            valid = (
                self.version >= 1
                and self.lease_token is None
                and self.lease_expires_at is None
                and self.failure_code is not None
                and self.completion is None
            )
        if not valid:
            raise ValueError("factor job status fields are inconsistent")
        return self

    def public(self) -> FactorJobRecord:
        return FactorJobRecord.model_validate(
            self.model_dump(mode="python", exclude={"lease_token"})
        )

    def lease(self) -> FactorJobLease:
        if self.status != "running" or self.lease_token is None or self.lease_expires_at is None:
            raise FactorLedgerLeaseError("factor job has no active lease")
        return FactorJobLease(
            job=self.public(),
            lease_token=self.lease_token,
            version=self.version,
            expires_at=self.lease_expires_at,
        )


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _canonical_model(model: BaseModel) -> str:
    return canonical_json_bytes(model.model_dump(mode="json", round_trip=True)).decode("utf-8")


def _utc(now: datetime) -> datetime:
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("factor ledger requires an aware clock")
    return now.astimezone(UTC)


def _system_utc() -> datetime:
    return datetime.now(UTC)


def _time(value: datetime | None) -> str | None:
    return None if value is None else _utc(value).isoformat(timespec="microseconds")


def _parsed_time(value: object, *, optional: bool = False) -> datetime | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError("factor ledger timestamp has an invalid type")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0) or _time(parsed) != value:
        raise ValueError("factor ledger timestamp is not canonical UTC")
    return parsed


def _model_from_json(model: type[BaseModel], value: object, max_bytes: int) -> BaseModel:
    if not isinstance(value, str) or not 0 < len(value.encode("utf-8")) <= max_bytes:
        raise ValueError("factor ledger persisted JSON exceeds its budget")
    strict_canonical_json_loads(value)
    parsed = model.model_validate_json(value)
    if model is FactorEvaluationCompletion and parsed.display_status == "display_unavailable":
        legacy = canonical_json_bytes(
            parsed.model_dump(
                mode="json",
                round_trip=True,
                exclude={
                    "display_artifact_sha256",
                    "display_artifact_filename",
                    "display_artifact_byte_count",
                },
            )
        ).decode("utf-8")
        if legacy != value:
            raise ValueError("factor legacy completion JSON is not canonical")
    elif _canonical_model(parsed) != value:
        raise ValueError("factor ledger persisted model JSON is not canonical")
    return parsed


def _payload(state: _JobState) -> dict[str, str | int | None]:
    return {
        "job_id": state.job_id,
        "spec_json": _canonical_model(state.spec),
        "spec_sha256": state.spec_sha256,
        "status": state.status,
        "version": state.version,
        "attempts": state.attempts,
        "created_at": _time(state.created_at),
        "updated_at": _time(state.updated_at),
        "lease_token": state.lease_token,
        "lease_expires_at": _time(state.lease_expires_at),
        "failure_code": state.failure_code,
        "completion_json": None if state.completion is None else _canonical_model(state.completion),
    }


_COLUMNS = (
    "job_id",
    "spec_json",
    "spec_sha256",
    "status",
    "version",
    "attempts",
    "created_at",
    "updated_at",
    "lease_token",
    "lease_expires_at",
    "failure_code",
    "completion_json",
)


def _decoded_job(
    row: sqlite3.Row, *, verified_spec: FactorEvaluationJobSpec | FactorStreamJobSpec | None = None
) -> _JobState:
    payload = {column: row[column] for column in _COLUMNS}
    try:
        if not isinstance(payload["job_id"], str) or _HEX32.fullmatch(payload["job_id"]) is None:
            raise ValueError("factor job ID is invalid")
        spec = (
            decode_factor_job_spec_json(payload["spec_json"])
            if verified_spec is None
            else verified_spec
        )
        if _canonical_model(spec) != payload["spec_json"]:
            raise ValueError("factor job spec JSON is not canonical")
        completion = (
            None
            if payload["completion_json"] is None
            else decode_factor_completion_json(payload["completion_json"])
        )
        if completion is not None:
            completion_model = (
                FactorStreamCompletion
                if isinstance(completion, FactorStreamCompletion)
                else FactorEvaluationCompletion
            )
            completion = _model_from_json(
                completion_model, payload["completion_json"], _MAX_COMPLETION_BYTES
            )
        created_at = _parsed_time(payload["created_at"])
        updated_at = _parsed_time(payload["updated_at"])
        lease_expires_at = _parsed_time(payload["lease_expires_at"], optional=True)
        state = _JobState.model_validate(
            dict(
                job_id=payload["job_id"],
                spec=spec,
                spec_sha256=payload["spec_sha256"],
                status=payload["status"],
                version=payload["version"],
                attempts=payload["attempts"],
                created_at=created_at,
                updated_at=updated_at,
                lease_token=payload["lease_token"],
                lease_expires_at=lease_expires_at,
                failure_code=payload["failure_code"],
                completion=completion,
            ),
            context=None if verified_spec is None else _VerifiedSpec(verified_spec),
        )
        if not isinstance(row["row_sha256"], str) or _digest(payload) != row["row_sha256"]:
            raise FactorLedgerIntegrityError("factor job row digest differs")
        return state
    except (TypeError, ValueError, ValidationError) as exc:
        raise FactorLedgerIntegrityError("factor job persistent state is invalid") from exc


@dataclass(frozen=True, slots=True)
class _CapturedJob:
    row: sqlite3.Row
    command: sqlite3.Row


def _command_payload(command_id: str, spec_sha256: str, job_id: str) -> dict[str, str]:
    return {"command_id": command_id, "spec_sha256": spec_sha256, "job_id": job_id}


def _checked_command(row: sqlite3.Row) -> tuple[str, str, str]:
    command_id, spec_sha256, job_id = (row[key] for key in ("command_id", "spec_sha256", "job_id"))
    payload = _command_payload(command_id, spec_sha256, job_id)
    if (
        not isinstance(command_id, str)
        or _COMMAND_PATTERN.fullmatch(command_id) is None
        or not isinstance(spec_sha256, str)
        or _HEX64.fullmatch(spec_sha256) is None
        or not isinstance(job_id, str)
        or _HEX32.fullmatch(job_id) is None
        or row["row_sha256"] != _digest(payload)
    ):
        raise FactorLedgerIntegrityError("factor command mapping is invalid")
    return command_id, spec_sha256, job_id


def _checked_completion(
    completion: FactorEvaluationCompletion,
    artifact: FactorResearchArtifactV1,
    display: FactorDisplayArtifactV1,
    spec: FactorEvaluationJobSpec,
    now: datetime,
) -> None:
    research = artifact.research
    source = research.receipt
    adapter = spec.adapter_request
    byte_count = len(canonical_json_bytes(artifact.model_dump(mode="json", round_trip=True)))
    display_byte_count = len(canonical_json_bytes(display.model_dump(mode="json", round_trip=True)))
    if (
        completion.spec_sha256 != spec.spec_sha256
        or completion.artifact_sha256 != artifact.content_sha256
        or completion.artifact_filename != f"factor-research-v1-{artifact.content_sha256}.json"
        or completion.artifact_byte_count != byte_count
        or completion.display_artifact_sha256 != display.content_sha256
        or completion.display_artifact_filename
        != f"factor-display-v1-{display.content_sha256}.json"
        or completion.display_artifact_byte_count != display_byte_count
        or completion.result_sha256 != research.result.sha256
        or completion.source_sha256 != source.source_sha256
        or completion.snapshot_id != source.snapshot_id
        or completion.binding_hash != source.binding_hash
        or completion.snapshot_as_of_time != source.snapshot_as_of_time
        or completion.source_mode != source.source_mode
        or completion.source_read_boundary != source.source_read_boundary
        or completion.visibility_basis != source.visibility_basis
        or completion.research_status != spec.research_status
        or completion.result_kind != spec.result_kind
        or completion.code_revision != spec.code_revision
        or artifact.code_revision != spec.code_revision
        or completion.completed_at > now
        or source.snapshot_id != spec.admission_request.snapshot_id
        or source.binding_hash != spec.admission_request.binding_hash
        or source.source_mode != spec.admission_request.source_mode
        or source.result_kind != spec.result_kind
        or source.pool_basis != adapter.pool_basis
        or source.stock_codes != adapter.stock_codes
        or source.query_start_date != adapter.query_start_date
        or source.query_end_date != adapter.query_end_date
        or source.evaluation_days != adapter.evaluation_days
        or research.request.factor_input.definition != adapter.definition
        or research.request.factor_input.universe != adapter.stock_codes
        or research.request.evaluation_days != adapter.evaluation_days
        or research.request.holding_sessions != adapter.holding_sessions
        or research.request.as_of != adapter.as_of
    ):
        raise FactorLedgerCompletionError("factor artifact differs from the exact job spec")
    if display != project_factor_display_artifact(artifact):
        raise FactorLedgerCompletionError("factor display differs from the complete artifact")


@dataclass(frozen=True, slots=True)
class _PreparedStream:
    instance: FactorLedgerIdentity
    job_id: str
    spec_sha256: str
    lease_token: str
    completion: FactorStreamCompletion
    witnesses: tuple[StreamArtifactWitness, ...]


class FactorEvaluationJobLedger:
    """Explicitly initialized ledger pinned to an out-of-band physical identity."""

    def __init__(self, path: Path, *, clock: Callable[[], datetime] = _system_utc) -> None:
        raw = os.fspath(path)
        if not isinstance(raw, str) or not raw.startswith("/") or os.path.normpath(raw) != raw:
            raise ValueError("factor ledger path must be canonical and absolute")
        if not callable(clock):
            raise TypeError("factor ledger requires a callable UTC clock")
        self.path = Path(raw)
        self._clock = clock
        self._expected: FactorLedgerIdentity | None = None
        self._prepared: dict[object, _PreparedStream] = {}
        self._prepared_lock = Lock()
        self._claim_spec = local()

    def _now(self) -> datetime:
        return _utc(self._clock())

    @staticmethod
    def _private_parent(path: Path) -> None:
        parent = path.parent
        observed = os.stat(parent, follow_symlinks=False)
        if (
            not stat.S_ISDIR(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) != 0o700
            or str(parent.resolve(strict=True)) != str(parent)
        ):
            raise FactorLedgerIdentityError("factor ledger parent must be a private real directory")

    def _file_identity(self) -> tuple[int, int]:
        self._private_parent(self.path)
        try:
            observed = os.stat(self.path, follow_symlinks=False)
        except FileNotFoundError as exc:
            raise FactorLedgerIdentityError("factor ledger file is missing") from exc
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) != 0o600
            or observed.st_nlink != 1
        ):
            raise FactorLedgerIdentityError("factor ledger file is not private and fixed")
        return observed.st_dev, observed.st_ino

    def _require_path(self, identity: FactorLedgerIdentity) -> None:
        if identity.path != str(self.path) or self._file_identity() != (
            identity.st_dev,
            identity.st_ino,
        ):
            raise FactorLedgerIdentityError("factor ledger file identity changed")

    @staticmethod
    def _require_schema(connection: sqlite3.Connection) -> str:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if version != _SCHEMA_VERSION or tables != _TABLES:
            raise FactorLedgerIntegrityError("factor ledger schema is incomplete or changed")
        identities = connection.execute(
            "SELECT instance_id FROM factor_ledger_identity ORDER BY singleton LIMIT 2"
        ).fetchall()
        if (
            len(identities) != 1
            or not isinstance(identities[0][0], str)
            or _HEX32.fullmatch(identities[0][0]) is None
        ):
            raise FactorLedgerIntegrityError("factor ledger instance row is invalid")
        return identities[0][0]

    def _expected_identity(self) -> FactorLedgerIdentity:
        if self._expected is None:
            raise FactorLedgerIdentityError("factor ledger requires an external identity")
        return self._expected

    def initialize(self) -> FactorLedgerIdentity:
        """Only create a new empty file; never infer an expected identity from an old path."""
        self._private_parent(self.path)
        try:
            descriptor = os.open(
                self.path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600,
            )
        except FileExistsError as exc:
            raise FactorLedgerIdentityError("factor ledger already exists") from exc
        try:
            os.fchmod(descriptor, 0o600)
            created = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        created_identity = (created.st_dev, created.st_ino)
        if self._file_identity() != created_identity:
            raise FactorLedgerIdentityError("factor ledger changed during initialization")
        instance_id = uuid4().hex
        connection = sqlite3.connect(
            f"{self.path.as_uri()}?mode=rw", uri=True, timeout=10, isolation_level=None
        )
        try:
            if self._file_identity() != created_identity:
                raise FactorLedgerIdentityError("factor ledger changed during initialization")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("BEGIN IMMEDIATE")
            for statement in (
                "CREATE TABLE factor_ledger_identity (singleton INTEGER PRIMARY KEY "
                "CHECK(singleton = 1), instance_id TEXT NOT NULL)",
                "CREATE TABLE factor_jobs (job_id TEXT PRIMARY KEY, spec_json TEXT NOT NULL, "
                "spec_sha256 TEXT NOT NULL UNIQUE, status TEXT NOT NULL, version INTEGER NOT NULL, "
                "attempts INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, "
                "lease_token TEXT, lease_expires_at TEXT, failure_code TEXT, completion_json TEXT, "
                "row_sha256 TEXT NOT NULL)",
                "CREATE TABLE factor_commands (command_id TEXT PRIMARY KEY, "
                "spec_sha256 TEXT NOT NULL, job_id TEXT NOT NULL REFERENCES factor_jobs(job_id), "
                "row_sha256 TEXT NOT NULL)",
            ):
                connection.execute(statement)
            connection.execute("INSERT INTO factor_ledger_identity VALUES (1, ?)", (instance_id,))
            connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            if self._file_identity() != created_identity:
                raise FactorLedgerIdentityError("factor ledger changed during initialization")
            connection.execute("COMMIT")
            if self._file_identity() != created_identity:
                raise FactorLedgerIdentityError("factor ledger changed during initialization")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()
        parent_fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        st_dev, st_ino = self._file_identity()
        if (st_dev, st_ino) != created_identity:
            raise FactorLedgerIdentityError("factor ledger changed during initialization")
        identity = FactorLedgerIdentity(
            instance_id=instance_id, path=str(self.path), st_dev=st_dev, st_ino=st_ino
        )
        self._expected = identity
        with self._reader():
            pass
        return identity

    @classmethod
    def open_existing(
        cls,
        expected_identity: FactorLedgerIdentity,
        *,
        clock: Callable[[], datetime] = _system_utc,
    ) -> FactorEvaluationJobLedger:
        """Open only with the saved external instance and physical file identity."""
        checked = FactorLedgerIdentity.model_validate(expected_identity.model_dump(mode="python"))
        ledger = cls(Path(checked.path), clock=clock)
        ledger._expected = checked
        with ledger._reader():
            pass
        return ledger

    @contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection]:
        identity = self._expected_identity()
        self._require_path(identity)
        try:
            connection = sqlite3.connect(
                f"{self.path.as_uri()}?mode=ro", uri=True, timeout=10, isolation_level=None
            )
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA query_only = ON")
                connection.execute("BEGIN")
                self._require_path(identity)
                if self._require_schema(connection) != identity.instance_id:
                    raise FactorLedgerIdentityError("factor ledger instance changed")
                yield connection
                self._require_path(identity)
            finally:
                connection.close()
        except sqlite3.DatabaseError as exc:
            raise FactorLedgerIntegrityError("factor ledger cannot be read") from exc

    @contextmanager
    def _writer(self) -> Iterator[sqlite3.Connection]:
        identity = self._expected_identity()
        self._require_path(identity)
        try:
            connection = sqlite3.connect(
                f"{self.path.as_uri()}?mode=rw", uri=True, timeout=10, isolation_level=None
            )
            connection.row_factory = sqlite3.Row
            try:
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute("PRAGMA synchronous = FULL")
                connection.execute("BEGIN IMMEDIATE")
                self._require_path(identity)
                if self._require_schema(connection) != identity.instance_id:
                    raise FactorLedgerIdentityError("factor ledger instance changed")
                yield connection
                self._require_path(identity)
                connection.execute("COMMIT")
                self._require_path(identity)
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()
        except sqlite3.DatabaseError as exc:
            raise FactorLedgerIntegrityError("factor ledger write failed") from exc

    @staticmethod
    def _capture_job(connection: sqlite3.Connection, job_id: str) -> _CapturedJob | None:
        row = connection.execute("SELECT * FROM factor_jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        anchor = connection.execute(
            "SELECT * FROM factor_commands WHERE job_id = ? ORDER BY command_id LIMIT 1",
            (job_id,),
        ).fetchone()
        if anchor is None:
            raise FactorLedgerIntegrityError("factor job has no command anchor")
        return _CapturedJob(row, anchor)

    @staticmethod
    def _decode_captured(
        captured: _CapturedJob,
        *,
        verified_spec: FactorEvaluationJobSpec | FactorStreamJobSpec | None = None,
    ) -> _JobState:
        state = (
            _decoded_job(captured.row)
            if verified_spec is None
            else _decoded_job(captured.row, verified_spec=verified_spec)
        )
        _, spec_sha256, linked_job_id = _checked_command(captured.command)
        if spec_sha256 != state.spec_sha256 or linked_job_id != state.job_id:
            raise FactorLedgerIntegrityError("factor command differs from job")
        return state

    @staticmethod
    def _load_job(connection: sqlite3.Connection, job_id: str) -> _JobState | None:
        captured = FactorEvaluationJobLedger._capture_job(connection, job_id)
        return None if captured is None else FactorEvaluationJobLedger._decode_captured(captured)

    @contextmanager
    def _reuse_claimed_spec(self, claimed: FactorJobLease) -> Iterator[None]:
        # A worker's heartbeat thread may retain only this fully checked immutable spec.
        previous = getattr(self._claim_spec, "value", None)
        self._claim_spec.value = _ClaimedSpec(
            self._expected_identity(),
            claimed.job.job_id,
            claimed.lease_token,
            claimed.version,
            claimed.job.spec,
        )
        try:
            yield
        finally:
            if previous is None:
                del self._claim_spec.value
            else:
                self._claim_spec.value = previous

    def _read_verified_job(
        self,
        job_id: str,
        *,
        verified_spec: FactorEvaluationJobSpec | FactorStreamJobSpec | None = None,
    ) -> _JobState | None:
        # A rollback-journal reader must release its snapshot before expensive decoding.
        with self._reader() as connection:
            captured = self._capture_job(connection, job_id)
        state = (
            None
            if captured is None
            else self._decode_captured(captured, verified_spec=verified_spec)
        )
        self._require_path(self._expected_identity())
        return state

    def _load_same_spec(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        spec: FactorEvaluationJobSpec | FactorStreamJobSpec,
    ) -> _JobState | None:
        captured = self._capture_job(connection, job_id)
        # Canonical spec bytes, typed state and row hash remain checked, together with
        # the command captured in this same transaction; only immutable redecoding is reused.
        return None if captured is None else self._decode_captured(captured, verified_spec=spec)

    @staticmethod
    def _insert_job(connection: sqlite3.Connection, state: _JobState) -> None:
        payload = _payload(state)
        columns = (*_COLUMNS, "row_sha256")
        placeholders = ", ".join("?" for _ in columns)
        connection.execute(
            f"INSERT INTO factor_jobs ({', '.join(columns)}) VALUES ({placeholders})",
            (*payload.values(), _digest(payload)),
        )

    @staticmethod
    def _store_job(connection: sqlite3.Connection, state: _JobState) -> None:
        payload = _payload(state)
        updated = connection.execute(
            "UPDATE factor_jobs SET "
            + ", ".join(f"{column} = ?" for column in _COLUMNS if column != "job_id")
            + ", row_sha256 = ? WHERE job_id = ?",
            (
                *[payload[column] for column in _COLUMNS if column != "job_id"],
                _digest(payload),
                state.job_id,
            ),
        )
        if updated.rowcount != 1:
            raise FactorLedgerIntegrityError("factor job vanished during update")

    def submit(
        self, command_id: str, spec: FactorEvaluationJobSpec | FactorStreamJobSpec
    ) -> FactorJobRecord:
        if not isinstance(command_id, str) or _COMMAND_PATTERN.fullmatch(command_id) is None:
            raise ValueError("factor command ID is invalid")
        checked = decode_factor_job_spec_json(_canonical_model(spec))
        with self._writer() as connection:
            current = self._now()
            command = connection.execute(
                "SELECT * FROM factor_commands WHERE command_id = ?", (command_id,)
            ).fetchone()
            if command is not None:
                _, digest, job_id = _checked_command(command)
                state = self._load_job(connection, job_id)
                if state is None or state.spec_sha256 != digest:
                    raise FactorLedgerIntegrityError("factor command points to another job")
                if digest != checked.spec_sha256:
                    raise FactorLedgerConflictError("factor command ID has different content")
                return state.public()
            existing = connection.execute(
                "SELECT job_id FROM factor_jobs WHERE spec_sha256 = ?", (checked.spec_sha256,)
            ).fetchone()
            if existing is None:
                if current >= checked.deadline:
                    raise FactorLedgerConflictError("factor job deadline has passed")
                state = _JobState(
                    job_id=uuid4().hex,
                    spec=checked,
                    spec_sha256=checked.spec_sha256,
                    status="queued",
                    version=0,
                    attempts=0,
                    created_at=current,
                    updated_at=current,
                    lease_token=None,
                    lease_expires_at=None,
                    failure_code=None,
                    completion=None,
                )
                self._insert_job(connection, state)
            else:
                state = self._load_job(connection, existing["job_id"])
                if (
                    state is None
                    or state.spec_sha256 != checked.spec_sha256
                    or state.spec != checked
                ):
                    raise FactorLedgerIntegrityError("factor job specification index is invalid")
            mapping = _command_payload(command_id, checked.spec_sha256, state.job_id)
            connection.execute(
                "INSERT INTO factor_commands (command_id, spec_sha256, job_id, row_sha256) "
                "VALUES (?, ?, ?, ?)",
                (*mapping.values(), _digest(mapping)),
            )
            return state.public()

    def command_exists(self, command_id: str) -> bool:
        """Any original command anchor prevents declaring an operation unsubmitted."""
        if not isinstance(command_id, str) or _COMMAND_PATTERN.fullmatch(command_id) is None:
            raise ValueError("factor command ID is invalid")
        with self._reader() as connection:
            return (
                connection.execute(
                    "SELECT 1 FROM factor_commands WHERE command_id = ?", (command_id,)
                ).fetchone()
                is not None
            )

    def lookup_command(self, command_id: str, spec_sha256: str) -> FactorJobRecord | None:
        """Read one original command anchor without creating or rebasing its ledger."""
        if not isinstance(command_id, str) or _COMMAND_PATTERN.fullmatch(command_id) is None:
            raise ValueError("factor command ID is invalid")
        if not isinstance(spec_sha256, str) or _HEX64.fullmatch(spec_sha256) is None:
            raise ValueError("factor command spec digest is invalid")
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM factor_commands WHERE command_id = ?", (command_id,)
            ).fetchone()
            if row is None:
                return None
            _, digest, job_id = _checked_command(row)
            state = self._load_job(connection, job_id)
            if state is None or state.spec_sha256 != digest:
                raise FactorLedgerIntegrityError("factor command points to another job")
            if digest != spec_sha256:
                raise FactorLedgerConflictError("factor command ID has different content")
            return state.public()

    def claim(self, *, lease_seconds: int, job_id: str | None = None) -> FactorJobLease | None:
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= _MAX_LEASE_SECONDS:
            raise ValueError("factor lease duration is invalid")
        if job_id is not None and (type(job_id) is not str or _HEX32.fullmatch(job_id) is None):
            raise ValueError("factor claim job ID is invalid")
        with self._writer() as connection:
            current = self._now()
            while True:
                if job_id is None:
                    candidate = connection.execute(
                        "SELECT job_id FROM factor_jobs WHERE status = 'queued' OR "
                        "(status = 'running' AND lease_expires_at <= ?) "
                        "ORDER BY created_at, job_id LIMIT 1",
                        (_time(current),),
                    ).fetchone()
                else:
                    candidate = connection.execute(
                        "SELECT job_id FROM factor_jobs WHERE job_id=? AND "
                        "(status = 'queued' OR (status = 'running' AND lease_expires_at <= ?))",
                        (job_id, _time(current)),
                    ).fetchone()
                if candidate is None:
                    return None
                state = self._load_job(connection, candidate["job_id"])
                if state is None:
                    raise FactorLedgerIntegrityError("factor claim candidate disappeared")
                if current < state.created_at:
                    raise ValueError("factor claim clock precedes submission")
                if current >= state.spec.deadline:
                    self._discard_job_prepared(state.job_id)
                    expired = state.model_copy(
                        update={
                            "status": "failed",
                            "version": state.version + 1,
                            "updated_at": current,
                            "lease_token": None,
                            "lease_expires_at": None,
                            "failure_code": "deadline_expired",
                        }
                    )
                    self._store_job(
                        connection, _JobState.model_validate(expired.model_dump(mode="python"))
                    )
                    continue
                claimed = state.model_copy(
                    update={
                        "status": "running",
                        "version": state.version + 1,
                        "attempts": state.attempts + 1,
                        "updated_at": current,
                        "lease_token": secrets.token_hex(32),
                        "lease_expires_at": min(
                            current + timedelta(seconds=lease_seconds), state.spec.deadline
                        ),
                    }
                )
                checked = _JobState.model_validate(claimed.model_dump(mode="python"))
                self._discard_job_prepared(state.job_id)
                self._store_job(connection, checked)
                return checked.lease()

    @staticmethod
    def _live_lease(
        state: _JobState, lease_token: str, expected_version: int, now: datetime
    ) -> None:
        if (
            state.status != "running"
            or not isinstance(lease_token, str)
            or _HEX64.fullmatch(lease_token) is None
            or not secrets.compare_digest(state.lease_token or "", lease_token)
            or type(expected_version) is not int
            or expected_version != state.version
            or state.lease_expires_at is None
            or now >= state.lease_expires_at
            or now >= state.spec.deadline
            or now < state.updated_at
        ):
            raise FactorLedgerLeaseError("factor lease is stale or expired")

    def heartbeat(
        self,
        job_id: str,
        lease_token: str,
        expected_version: int,
        lease_seconds: int,
    ) -> FactorJobLease:
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= _MAX_LEASE_SECONDS:
            raise ValueError("factor lease duration is invalid")
        claimed = getattr(self._claim_spec, "value", None)
        if (
            isinstance(claimed, _ClaimedSpec)
            and claimed.instance == self._expected_identity()
            and claimed.job_id == job_id
            and claimed.lease_token == lease_token
            and type(expected_version) is int
            and expected_version >= claimed.version
        ):
            prior = self._read_verified_job(job_id, verified_spec=claimed.spec)
        else:
            prior = self._read_verified_job(job_id)
        if prior is None:
            raise FactorLedgerLeaseError("factor job does not exist")
        with self._writer() as connection:
            state = self._load_same_spec(connection, job_id, prior.spec)
            if state is None:
                raise FactorLedgerLeaseError("factor job does not exist")
            current = self._now()
            self._live_lease(state, lease_token, expected_version, current)
            extended = state.model_copy(
                update={
                    "version": state.version + 1,
                    "updated_at": current,
                    "lease_expires_at": min(
                        current + timedelta(seconds=lease_seconds), state.spec.deadline
                    ),
                }
            )
            fields = extended.model_dump(mode="python")
            fields["spec"] = state.spec
            checked = _JobState.model_validate(fields, context=_VerifiedSpec(state.spec))
            self._store_job(connection, checked)
            self._live_lease(state, lease_token, expected_version, self._now())
        # COMMIT and fully typed public construction can both consume the lease.
        # A loss after commit leaves the original recoverable row; never force an undo.
        self._live_lease(state, lease_token, expected_version, self._now())
        result = checked.lease()
        self._require_path(self._expected_identity())
        self._live_lease(checked, lease_token, checked.version, self._now())
        return result

    def complete(
        self,
        job_id: str,
        lease_token: str,
        expected_version: int,
        completion: FactorEvaluationCompletion,
        artifact_root: Path,
    ) -> FactorJobRecord:
        try:
            checked_completion = FactorEvaluationCompletion.model_validate(
                completion.model_dump(mode="python")
            )
            if checked_completion.display_status != "available":
                raise FactorLedgerCompletionError("new factor success requires a display artifact")
            artifact, artifact_root_identity, artifact_identity = (
                _load_factor_research_artifact_with_identity(
                    artifact_root, checked_completion.artifact_sha256
                )
            )
            display, display_root_identity, display_identity = (
                _load_factor_display_artifact_with_identity(
                    artifact_root, checked_completion.display_artifact_sha256
                )
            )
            if artifact_root_identity != display_root_identity:
                raise FactorLedgerCompletionError(
                    "factor artifact roots changed during verification"
                )
        except (OSError, TypeError, ValueError, ValidationError) as exc:
            raise FactorLedgerCompletionError("factor result artifact cannot be verified") from exc
        with self._writer() as connection:
            current = self._now()
            state = self._load_job(connection, job_id)
            if state is None:
                raise FactorLedgerLeaseError("factor job does not exist")
            self._live_lease(state, lease_token, expected_version, current)
            _checked_completion(checked_completion, artifact, display, state.spec, current)
            try:
                latest_artifact, latest_root, latest_identity = (
                    _load_factor_research_artifact_with_identity(
                        artifact_root, checked_completion.artifact_sha256
                    )
                )
                latest_display, latest_display_root, latest_display_identity = (
                    _load_factor_display_artifact_with_identity(
                        artifact_root, checked_completion.display_artifact_sha256
                    )
                )
                if (
                    latest_artifact != artifact
                    or latest_display != display
                    or latest_root != artifact_root_identity
                    or latest_display_root != display_root_identity
                    or latest_identity != artifact_identity
                    or latest_display_identity != display_identity
                ):
                    raise FactorLedgerCompletionError("factor artifact changed before completion")
            except (OSError, ValueError, ValidationError) as exc:
                raise FactorLedgerCompletionError(
                    "factor artifact changed before completion"
                ) from exc
            current = self._now()
            self._live_lease(state, lease_token, expected_version, current)
            if checked_completion.completed_at > current:
                raise FactorLedgerCompletionError("factor completion time exceeds ledger time")
            succeeded = state.model_copy(
                update={
                    "status": "succeeded",
                    "version": state.version + 1,
                    "updated_at": current,
                    "lease_token": None,
                    "lease_expires_at": None,
                    "completion": checked_completion,
                }
            )
            checked = _JobState.model_validate(succeeded.model_dump(mode="python"))
            self._store_job(connection, checked)
        return checked.public()

    def discard_prepared_stream(self, handle: object) -> None:
        """Release this instance's private evidence after cancellation or loss."""
        with self._prepared_lock:
            if type(handle) is object:
                self._prepared.pop(handle, None)

    def _discard_job_prepared(self, job_id: str, *, lease_token: str | None = None) -> None:
        with self._prepared_lock:
            for handle, prepared in tuple(self._prepared.items()):
                if prepared.job_id == job_id and (
                    lease_token is None or prepared.lease_token == lease_token
                ):
                    del self._prepared[handle]

    def _prepared_stream(self, handle: object) -> _PreparedStream:
        with self._prepared_lock:
            prepared = self._prepared.get(handle) if type(handle) is object else None
            if prepared is None or prepared.instance != self._expected:
                raise FactorLedgerCompletionError(
                    "stream prepared handle is not owned by this ledger instance"
                )
            return prepared

    def prepare_stream_completion(
        self,
        job_id: str,
        lease_token: str,
        completion: FactorStreamCompletion,
        artifact_root: Path,
        member_root: Path,
    ) -> object:
        """Long verification outside the writer, while the worker keeps renewing."""
        prior = self._read_verified_job(job_id)
        if prior is None or not isinstance(prior.spec, FactorStreamJobSpec):
            raise FactorLedgerCompletionError("stream preparation requires a v2 job")
        with self._reader() as connection:
            state = self._load_same_spec(connection, job_id, prior.spec)
            if state is None:
                raise FactorLedgerLeaseError("stream job disappeared during preparation")
            self._live_lease(state, lease_token, state.version, self._now())
            spec = state.spec
        self._discard_job_prepared(job_id, lease_token=lease_token)
        self._prune_prepared_streams()
        try:
            checked = FactorStreamCompletion.model_validate(completion.model_dump(mode="python"))
            verified = verify_factor_stream_artifacts(spec, checked, artifact_root, member_root)
        except (OSError, TypeError, ValueError, ValidationError) as exc:
            raise FactorLedgerCompletionError(
                "stream journal completion cannot be verified"
            ) from exc
        with self._reader() as connection:
            state = self._load_same_spec(connection, job_id, spec)
            if state is None or state.spec != spec:
                raise FactorLedgerLeaseError("stream job changed during preparation")
            self._live_lease(state, lease_token, state.version, self._now())
            if checked.completed_at > self._now():
                raise FactorLedgerCompletionError(
                    "stream completion time exceeds trusted ledger time"
                )
        assert self._expected is not None
        handle = object()
        with self._prepared_lock:
            for previous, record in tuple(self._prepared.items()):
                if record.job_id == job_id and record.lease_token == lease_token:
                    del self._prepared[previous]
            if len(self._prepared) >= _MAX_LIST:
                raise FactorLedgerCompletionError("stream prepared record capacity exceeded")
            self._prepared[handle] = _PreparedStream(
                self._expected, job_id, spec.spec_sha256, lease_token, checked, verified.witnesses
            )
        return handle

    def _prune_prepared_streams(self) -> None:
        with self._prepared_lock:
            records = tuple(self._prepared.items())
        if not records:
            return
        stale = []
        try:
            with self._reader() as connection:
                current = self._now()
                for handle, prepared in records:
                    state = self._load_job(connection, prepared.job_id)
                    if state is None or state.spec_sha256 != prepared.spec_sha256:
                        stale.append(handle)
                        continue
                    try:
                        self._live_lease(state, prepared.lease_token, state.version, current)
                    except FactorLedgerLeaseError:
                        stale.append(handle)
        except BaseException:
            with self._prepared_lock:
                self._prepared.clear()
            raise
        for handle in stale:
            self.discard_prepared_stream(handle)

    def complete_prepared_stream(
        self, job_id: str, lease_token: str, expected_version: int, handle: object
    ) -> FactorJobRecord:
        """Only identities and original lease/CAS checks occur after heartbeat stops."""
        try:
            prepared = self._prepared_stream(handle)
            if prepared.job_id != job_id or not secrets.compare_digest(
                prepared.lease_token, lease_token
            ):
                raise FactorLedgerCompletionError("stream prepared handle claim differs")
            with self._writer() as connection:
                # BEGIN IMMEDIATE may have waited: all named identities must be checked HERE.
                if self._prepared_stream(handle) is not prepared:
                    raise FactorLedgerCompletionError("stream prepared evidence was discarded")
                try:
                    for witness in prepared.witnesses:
                        witness.recheck()
                except (OSError, ValueError) as exc:
                    raise FactorLedgerCompletionError(
                        "stream artifacts changed before CAS"
                    ) from exc
                state = self._load_job(connection, job_id)
                if (
                    state is None
                    or not isinstance(state.spec, FactorStreamJobSpec)
                    or state.spec_sha256 != prepared.spec_sha256
                ):
                    raise FactorLedgerCompletionError("stream prepared job binding differs")
                current = self._now()
                self._live_lease(state, lease_token, expected_version, current)
                if prepared.completion.completed_at > current:
                    raise FactorLedgerCompletionError(
                        "stream completion time exceeds trusted ledger time"
                    )
                succeeded = state.model_copy(
                    update={
                        "status": "succeeded",
                        "version": state.version + 1,
                        "updated_at": current,
                        "lease_token": None,
                        "lease_expires_at": None,
                        "completion": prepared.completion,
                    }
                )
                checked = _JobState.model_validate(succeeded.model_dump(mode="python"))
                self._store_job(connection, checked)
                return checked.public()
        finally:
            self.discard_prepared_stream(handle)

    def fail(
        self,
        job_id: str,
        lease_token: str,
        expected_version: int,
        reason_code: FactorJobFailureCode,
    ) -> FactorJobRecord:
        if reason_code not in (
            "source_unavailable",
            "artifact_invalid",
            "evaluation_failed",
            "internal_error",
        ):
            raise ValueError("factor failure reason is not allowed")
        with self._writer() as connection:
            current = self._now()
            state = self._load_job(connection, job_id)
            if state is None:
                raise FactorLedgerLeaseError("factor job does not exist")
            self._live_lease(state, lease_token, expected_version, current)
            failed = state.model_copy(
                update={
                    "status": "failed",
                    "version": state.version + 1,
                    "updated_at": current,
                    "lease_token": None,
                    "lease_expires_at": None,
                    "failure_code": reason_code,
                }
            )
            checked = _JobState.model_validate(failed.model_dump(mode="python"))
            self._discard_job_prepared(job_id)
            self._store_job(connection, checked)
            return checked.public()

    def get(self, job_id: str) -> FactorJobRecord | None:
        if not isinstance(job_id, str) or _HEX32.fullmatch(job_id) is None:
            raise ValueError("factor job ID is invalid")
        self._prune_prepared_streams()
        with self._reader() as connection:
            state = self._load_job(connection, job_id)
            return None if state is None else state.public()

    def list_recent(self, *, limit: int = 50) -> tuple[FactorJobRecord, ...]:
        if type(limit) is not int or not 1 <= limit <= _MAX_LIST:
            raise ValueError("factor job list limit is invalid")
        with self._reader() as connection:
            identifiers = connection.execute(
                "SELECT job_id FROM factor_jobs ORDER BY created_at DESC, job_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return tuple(
                state.public()
                for row in identifiers
                if (state := self._load_job(connection, row["job_id"])) is not None
            )

    def list_recent_updated(self, *, limit: int = 50) -> tuple[FactorJobRecord, ...]:
        """Read the bounded latest-changing jobs for a stable result publication."""
        if type(limit) is not int or not 1 <= limit <= _MAX_LIST:
            raise ValueError("factor job list limit is invalid")
        with self._reader() as connection:
            identifiers = connection.execute(
                "SELECT job_id FROM factor_jobs ORDER BY updated_at DESC, job_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return tuple(
                state.public()
                for row in identifiers
                if (state := self._load_job(connection, row["job_id"])) is not None
            )
