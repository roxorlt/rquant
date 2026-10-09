"""SQLite state for resumable historical backfill manifests."""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal, TypeAlias, cast
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

if TYPE_CHECKING:
    from rquant.backfill_execute_contracts import (
        BackfillExecutionIntent,
        BackfillExecutionSpec,
        MaintenanceExecutionStatus,
    )
    from rquant.financial_runtime import FinancialExecutionIntent, FinancialExecutionSpec

BackfillTaskStatus: TypeAlias = Literal["pending", "running", "succeeded", "failed"]
BackfillManifestStatusValue: TypeAlias = Literal[
    "pending",
    "running",
    "completed",
    "failed",
    "abandoned",
]
JsonObject: TypeAlias = dict[str, JsonValue]


class ManifestContentConflictError(RuntimeError):
    """An existing manifest ID points at different canonical content."""


class UnknownManifestError(LookupError):
    """The requested manifest is not present in the state database."""


class UnknownTaskError(LookupError):
    """The requested manifest task is not present in the state database."""


class StaleTaskClaimError(RuntimeError):
    """A worker tried to finish a task through an expired or replaced claim."""


class ManifestAbandonedError(RuntimeError):
    """A worker tried to execute an intentionally abandoned manifest."""


class ManifestAbandonmentConflictError(RuntimeError):
    """A manifest cannot be abandoned in its current state."""


class StaleManifestAbandonmentError(RuntimeError):
    """The manifest changed after its abandonment plan was prepared."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _normalize_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("backfill state timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _encode_time(value: datetime) -> str:
    return _normalize_time(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _decode_time(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _json_dumps(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _json_object(value: str) -> JsonObject:
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise ValueError("stored backfill JSON payload must be an object")
    return cast(JsonObject, decoded)


class BackfillStateModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class BackfillTaskInput(BackfillStateModel):
    task_id: str = Field(min_length=1)
    payload: JsonObject
    max_attempts: int = Field(default=3, ge=1)


class BackfillEligibilityInput(BackfillStateModel):
    eligibility_id: str = Field(min_length=1)
    payload: JsonObject


class BackfillManifestInput(BackfillStateModel):
    manifest_id: str = Field(min_length=1)
    payload: JsonObject
    tasks: tuple[BackfillTaskInput, ...]
    eligibility: tuple[BackfillEligibilityInput, ...]

    @model_validator(mode="after")
    def validate_child_ids(self) -> BackfillManifestInput:
        task_ids = [task.task_id for task in self.tasks]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("manifest task IDs must be unique")
        eligibility_ids = [row.eligibility_id for row in self.eligibility]
        if len(eligibility_ids) != len(set(eligibility_ids)):
            raise ValueError("manifest eligibility IDs must be unique")
        return self


class BackfillFailure(BackfillStateModel):
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    retryable: bool
    details: JsonObject = Field(default_factory=dict)


class BackfillTaskMetrics(BackfillStateModel):
    request_count: int = Field(default=0, ge=0)
    returned_rows: int = Field(default=0, ge=0)
    written_rows: int = Field(default=0, ge=0)
    covered_sessions: int = Field(default=0, ge=0)
    allowed_missing_sessions: int = Field(default=0, ge=0)


class ClaimedBackfillTask(BackfillStateModel):
    manifest_id: str
    task_id: str
    ordinal: int = Field(ge=0)
    payload: JsonObject
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1)
    worker_id: str
    claim_token: str
    recovery_only: bool = False
    claimed_at: datetime
    lease_expires_at: datetime

    @field_validator("claimed_at", "lease_expires_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _normalize_time(value)


class BackfillTaskState(BackfillStateModel):
    manifest_id: str
    task_id: str
    payload: JsonObject
    status: BackfillTaskStatus
    attempts: int = Field(ge=0)
    max_attempts: int = Field(ge=1)
    worker_id: str | None = None
    claim_token: str | None = None
    claimed_at: datetime | None = None
    lease_expires_at: datetime | None = None
    failure: BackfillFailure | None = None
    metrics: BackfillTaskMetrics = Field(default_factory=BackfillTaskMetrics)
    duration_seconds: float | None = None
    finished_at: datetime | None = None

    @field_validator("claimed_at", "lease_expires_at", "finished_at")
    @classmethod
    def normalize_optional_timestamp(cls, value: datetime | None) -> datetime | None:
        return None if value is None else _normalize_time(value)


class BackfillTaskFailureSummary(BackfillStateModel):
    task_id: str
    attempts: int
    max_attempts: int
    failure: BackfillFailure


class BackfillManifestTermination(BackfillStateModel):
    reason: str = Field(min_length=1)
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    terminated_at: datetime
    plan_id: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("terminated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _normalize_time(value)


class BackfillManifestAbandonmentPlan(BackfillStateModel):
    action_id: Literal["backfill-manifest-abandon/v1"] = (
        "backfill-manifest-abandon/v1"
    )
    manifest_id: str = Field(min_length=1)
    manifest_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    manifest_updated_at: datetime
    status_before: Literal["pending", "running", "failed"]
    task_count: int = Field(ge=0)
    pending: int = Field(ge=0)
    running: int = Field(ge=0)
    succeeded: int = Field(ge=0)
    failed: int = Field(ge=0)
    reason: str = Field(min_length=1)
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    plan_id: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("manifest_updated_at")
    @classmethod
    def normalize_timestamp(cls, value: datetime) -> datetime:
        return _normalize_time(value)


class BackfillManifestStatus(BackfillStateModel):
    manifest_id: str
    status: BackfillManifestStatusValue
    terminal: bool
    task_count: int
    eligibility_count: int
    pending: int
    running: int
    succeeded: int
    failed: int
    ewma_duration_seconds: float | None
    eta_seconds: float | None
    request_count: int
    returned_rows: int
    written_rows: int
    covered_sessions: int
    allowed_missing_sessions: int
    failures: tuple[BackfillTaskFailureSummary, ...]
    termination: BackfillManifestTermination | None = None


class ProtectedBackfillClaim:
    def __init__(
        self, store: BackfillStateStore, connection: sqlite3.Connection,
        claim: ClaimedBackfillTask,
        guard: Callable[[sqlite3.Connection,datetime],None] | None,
    ) -> None:
        self.store, self.connection, self.claim, self.guard = store,connection,claim,guard

    def verify(self, *, now: datetime | None = None) -> None:
        observed = _normalize_time(now or _utc_now())
        if not self.connection.in_transaction:
            raise StaleTaskClaimError('commit protection is no longer active')
        self.store._require_manifest_active(self.connection,self.claim.manifest_id)
        self.store._require_current_claim(self.connection,self.claim)
        row = self.connection.execute('''
            SELECT lease_expires_at FROM backfill_task WHERE manifest_id=? AND task_id=?
        ''',(self.claim.manifest_id,self.claim.task_id)).fetchone()
        expiry = None if row is None else _decode_time(row['lease_expires_at'])
        if expiry is None or expiry <= observed:
            raise StaleTaskClaimError('claim lease expired before fact commit')
        if self.guard is not None:
            self.guard(self.connection,observed)

    def succeed(
        self, *, duration_seconds: float, metrics: BackfillTaskMetrics | None = None,
        now: datetime | None = None,
    ) -> None:
        self.verify(now=now)
        self.store._succeed_in_transaction(self.connection,self.claim,
            duration_seconds=duration_seconds,metrics=metrics,now=now)


class BackfillWorkloadTelemetry(BackfillStateModel):
    remaining_tasks: int = Field(ge=0)
    remaining_expected_rows: int = Field(ge=0)
    sample_task_count: int = Field(ge=0)
    sample_manifest_count: int = Field(ge=0)
    p75_task_seconds: float | None = Field(default=None, ge=0)
    p75_seconds_per_row: float | None = Field(default=None, ge=0)


def _linear_quantile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    if not 0 <= quantile <= 1:
        raise ValueError("quantile must be between zero and one")
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


@contextmanager
def open_backfill_state_snapshot(
    path: Path | str | None = None,
    *,
    busy_timeout_ms: int | None = None,
) -> Iterator[BackfillStateStore]:
    """Yield an immutable point-in-time copy of the live SQLite state."""
    if path is None or busy_timeout_ms is None:
        from rquant.config import settings

        if path is None:
            path = settings.backfill_state_path_resolved
        if busy_timeout_ms is None:
            busy_timeout_ms = settings.backfill_state_busy_timeout_ms
    if busy_timeout_ms < 1:
        raise ValueError("busy_timeout_ms must be positive")

    source_path = Path(path)
    if not source_path.is_file() or source_path.is_symlink():
        raise ValueError(f"backfill state database is invalid: {source_path}")

    with TemporaryDirectory(prefix="rquant-backfill-state-") as directory:
        snapshot_path = Path(directory) / source_path.name
        source = sqlite3.connect(
            f"{source_path.resolve().as_uri()}?mode=ro",
            uri=True,
            timeout=busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        target = sqlite3.connect(
            snapshot_path,
            timeout=busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        try:
            source.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            target.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
            source.backup(target)
        finally:
            target.close()
            source.close()

        yield BackfillStateStore(
            snapshot_path,
            busy_timeout_ms=busy_timeout_ms,
            read_only=True,
        )


class BackfillStateStore:
    """Keep backfill orchestration state outside the write-heavy DuckDB file."""

    def __init__(
        self,
        path: Path | str | None = None,
        *,
        busy_timeout_ms: int | None = None,
        ewma_alpha: float = 0.3,
        read_only: bool = False,
        maintenance_enabled: bool = False,
    ) -> None:
        if path is None or busy_timeout_ms is None:
            from rquant.config import settings

            if path is None:
                path = settings.backfill_state_path_resolved
            if busy_timeout_ms is None:
                busy_timeout_ms = settings.backfill_state_busy_timeout_ms
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        if not 0 < ewma_alpha <= 1:
            raise ValueError("ewma_alpha must be in (0, 1]")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.ewma_alpha = ewma_alpha
        self.read_only = read_only
        self.maintenance_enabled = maintenance_enabled
        if read_only:
            if not self.path.is_file() or self.path.is_symlink():
                raise ValueError(
                    f"read-only backfill state database is invalid: {self.path}"
                )
            self._require_checkpointed_snapshot()
        else:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()
            if maintenance_enabled:
                self._initialize_maintenance()

    def _require_checkpointed_snapshot(self) -> None:
        sidecars = tuple(
            self.path.with_name(f"{self.path.name}{suffix}")
            for suffix in ("-wal", "-shm", "-journal")
        )
        if any(path.is_symlink() or path.exists() for path in sidecars):
            raise ValueError(
                "read-only backfill state database must be checkpointed"
            )

    def _connect(self) -> sqlite3.Connection:
        target: Path | str = self.path
        connect_kwargs: dict[str, object] = {}
        if self.read_only:
            self._require_checkpointed_snapshot()
            target = (
                f"{self.path.resolve().as_uri()}?mode=ro&immutable=1"
            )
            connect_kwargs["uri"] = True
        connection = sqlite3.connect(
            target,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            **connect_kwargs,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys = ON")
        if self.read_only:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("PRAGMA temp_store = MEMORY")
        return connection

    @contextmanager
    def _write_transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode = WAL")
        finally:
            connection.close()
        with self._write_transaction() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_manifest (
                    manifest_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    ewma_duration_seconds REAL
                )
                """
            )
            manifest_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(backfill_manifest)"
                ).fetchall()
            }
            manifest_additions = {
                "terminal_status": "TEXT",
                "termination_reason": "TEXT",
                "terminated_at": "TEXT",
                "terminated_by_commit": "TEXT",
                "termination_plan_id": "TEXT",
            }
            for column_name, column_type in manifest_additions.items():
                if column_name not in manifest_columns:
                    connection.execute(
                        f"ALTER TABLE backfill_manifest "
                        f"ADD COLUMN {column_name} {column_type}"
                    )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_task (
                    manifest_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    status TEXT NOT NULL CHECK (
                        status IN ('pending', 'running', 'succeeded', 'failed')
                    ),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
                    recovery_attempted INTEGER NOT NULL DEFAULT 0 CHECK (
                        recovery_attempted IN (0, 1)
                    ),
                    worker_id TEXT,
                    claim_token TEXT,
                    claimed_at TEXT,
                    lease_seconds INTEGER,
                    lease_expires_at TEXT,
                    failure_json TEXT,
                    metrics_json TEXT,
                    duration_seconds REAL,
                    finished_at TEXT,
                    PRIMARY KEY (manifest_id, task_id),
                    FOREIGN KEY (manifest_id) REFERENCES backfill_manifest(manifest_id)
                        ON DELETE CASCADE
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS backfill_eligibility (
                    manifest_id TEXT NOT NULL,
                    eligibility_id TEXT NOT NULL,
                    ordinal INTEGER NOT NULL,
                    payload_json TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    PRIMARY KEY (manifest_id, eligibility_id),
                    FOREIGN KEY (manifest_id) REFERENCES backfill_manifest(manifest_id)
                        ON DELETE CASCADE
                )
                """
            )
            task_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(backfill_task)"
                ).fetchall()
            }
            if "metrics_json" not in task_columns:
                connection.execute(
                    "ALTER TABLE backfill_task ADD COLUMN metrics_json TEXT"
                )
            if "recovery_attempted" not in task_columns:
                connection.execute(
                    "ALTER TABLE backfill_task "
                    "ADD COLUMN recovery_attempted INTEGER NOT NULL DEFAULT 0"
                )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_backfill_task_claim
                ON backfill_task (manifest_id, status, ordinal)
                """
            )

    @staticmethod
    def _canonical_manifest(manifest: BackfillManifestInput) -> JsonObject:
        tasks = [
            task.model_dump(mode="json")
            for task in sorted(manifest.tasks, key=lambda item: item.task_id)
        ]
        eligibility = [
            row.model_dump(mode="json")
            for row in sorted(manifest.eligibility, key=lambda item: item.eligibility_id)
        ]
        return {
            "payload": manifest.payload,
            "tasks": tasks,
            "eligibility": eligibility,
        }

    @staticmethod
    def _content_hash(value: object) -> str:
        return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()

    def persist_manifest(
        self,
        manifest: BackfillManifestInput,
        *,
        now: datetime | None = None,
    ) -> None:
        persisted_at = _encode_time(now or _utc_now())
        manifest_hash = self._content_hash(self._canonical_manifest(manifest))
        with self._write_transaction() as connection:
            self._persist_manifest_on(connection,manifest,persisted_at,manifest_hash)

    def _persist_manifest_on(
        self, connection: sqlite3.Connection, manifest: BackfillManifestInput,
        persisted_at: str, manifest_hash: str,
    ) -> None:
        if not connection.in_transaction:
            raise ValueError('manifest requires the original state transaction')
        existing = connection.execute(
            "SELECT content_hash FROM backfill_manifest WHERE manifest_id = ?",
            (manifest.manifest_id,),
        ).fetchone()
        if existing is not None:
            if existing["content_hash"] != manifest_hash:
                raise ManifestContentConflictError(
                    f"manifest {manifest.manifest_id!r} already has different content"
                )
            return

        connection.execute(
            """
            INSERT INTO backfill_manifest (
                manifest_id, payload_json, content_hash, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                manifest.manifest_id,
                _json_dumps(manifest.payload),
                manifest_hash,
                persisted_at,
                persisted_at,
            ),
        )
        for ordinal, task in enumerate(sorted(manifest.tasks, key=lambda item: item.task_id)):
            task_json = _json_dumps(task.payload)
            connection.execute(
                """
                INSERT INTO backfill_task (
                    manifest_id, task_id, ordinal, payload_json, content_hash,
                    status, max_attempts
                ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    manifest.manifest_id,
                    task.task_id,
                    ordinal,
                    task_json,
                    self._content_hash(task.model_dump(mode="json")),
                    task.max_attempts,
                ),
            )
        for ordinal, row in enumerate(
            sorted(manifest.eligibility, key=lambda item: item.eligibility_id)
        ):
            payload_json = _json_dumps(row.payload)
            connection.execute(
                """
                INSERT INTO backfill_eligibility (
                    manifest_id, eligibility_id, ordinal, payload_json, content_hash
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    manifest.manifest_id,
                    row.eligibility_id,
                    ordinal,
                    payload_json,
                    self._content_hash(row.model_dump(mode="json")),
                ),
            )

    def _initialize_maintenance(self) -> None:
        with self._write_transaction() as connection:
            connection.execute('''CREATE TABLE IF NOT EXISTS data_center_maintenance_intent (
                intent_id TEXT PRIMARY KEY, prepare_command_id TEXT NOT NULL UNIQUE,
                execution_id TEXT NOT NULL, owner TEXT NOT NULL, payload_json TEXT NOT NULL,
                content_hash TEXT NOT NULL, expires_at TEXT NOT NULL, consumed_command_id TEXT
            )''')
            connection.execute(
                'CREATE TABLE IF NOT EXISTS data_center_maintenance_execution (\n'
                '                execution_id TEXT PRIMARY KEY, owner TEXT NOT NULL, '
                'manifest_id TEXT NOT NULL UNIQUE,\n'
                '                primary_generation TEXT NOT NULL, spec_json TEXT NOT NULL, '
                'spec_hash TEXT NOT NULL,\n'
                '                status TEXT NOT NULL, status_json TEXT NOT NULL,\n'
                '                FOREIGN KEY(manifest_id) '
                'REFERENCES backfill_manifest(manifest_id)\n'
                '            )')
            connection.execute('''CREATE UNIQUE INDEX IF NOT EXISTS data_center_maintenance_active
                ON data_center_maintenance_execution(primary_generation)
                WHERE status IN ('queued','running','verifying')''')
            connection.execute('''CREATE TABLE IF NOT EXISTS data_center_maintenance_control (
                command_id TEXT PRIMARY KEY, execution_id TEXT NOT NULL, payload_json TEXT NOT NULL,
                result_json TEXT NOT NULL,
                FOREIGN KEY(execution_id) REFERENCES data_center_maintenance_execution(execution_id)
            )''')

    def _require_maintenance(self) -> None:
        if not self.maintenance_enabled:
            raise ValueError('controlled maintenance is not configured')

    def maintenance_intent_by_command(
            self,command_id: str,*,owner: str
    ) -> BackfillExecutionIntent | None:
        from rquant.backfill_execute_contracts import BackfillExecutionIntent
        self._require_maintenance()
        connection=self._connect()
        try:
            row=connection.execute(
                'SELECT owner,payload_json,content_hash FROM data_center_maintenance_intent '
                'WHERE prepare_command_id=?',
                (command_id,)).fetchone()
            if row is None:
                return None
            if row['owner']!=owner:
                raise ValueError('execution confirmation belongs to another owner')
            intent=BackfillExecutionIntent.model_validate_json(row['payload_json'])
            if intent.intent_id!=row['content_hash']:
                raise ValueError('execution confirmation content changed')
            return intent
        finally:
            connection.close()

    def persist_maintenance_intent(
            self,intent: BackfillExecutionIntent | FinancialExecutionIntent
    ) -> BackfillExecutionIntent | FinancialExecutionIntent:
        from rquant.backfill_execute_contracts import BackfillExecutionIntent
        from rquant.financial_runtime import FinancialExecutionIntent
        self._require_maintenance()
        model=(FinancialExecutionIntent
            if isinstance(intent,FinancialExecutionIntent) else BackfillExecutionIntent)
        intent=model.model_validate_json(intent.model_dump_json())
        payload=intent.model_dump_json()
        if len(payload.encode())>8_000_000:
            raise ValueError('execution confirmation exceeds bounded capacity')
        with self._write_transaction() as connection:
            old=connection.execute(
                'SELECT payload_json FROM data_center_maintenance_intent '
                'WHERE prepare_command_id=?',
                (intent.prepare_command_id,)).fetchone()
            if old is not None:
                if old['payload_json']!=payload:
                    raise ValueError('same confirmation command has different content')
                return intent
            if connection.execute(
                    'SELECT COUNT(*) FROM data_center_maintenance_intent').fetchone()[0]>=4096:
                raise ValueError('execution confirmation history capacity reached')
            connection.execute('''INSERT INTO data_center_maintenance_intent
                (intent_id,prepare_command_id,execution_id,owner,payload_json,content_hash,expires_at)
                VALUES (?,?,?,?,?,?,?)''',
                (intent.intent_id,intent.prepare_command_id,intent.execution_id,intent.owner,
                payload,intent.intent_id,_encode_time(intent.expires_at)))
        return intent

    def financial_intent_by_command(
            self,command_id: str,*,owner: str
    ) -> FinancialExecutionIntent | None:
        from rquant.financial_runtime import FinancialExecutionIntent
        self._require_maintenance()
        connection=self._connect()
        try:
            row=connection.execute(
                'SELECT owner,payload_json,content_hash FROM data_center_maintenance_intent '
                'WHERE prepare_command_id=?',
                (command_id,)).fetchone()
            if row is None:
                return None
            if row['owner']!=owner or len(row['payload_json'].encode())>8_000_000:
                raise ValueError('financial confirmation owner or capacity differs')
            intent=FinancialExecutionIntent.model_validate_json(row['payload_json'])
            if intent.intent_id!=row['content_hash']:
                raise ValueError('financial confirmation content changed')
            return intent
        finally:
            connection.close()

    @staticmethod
    def _maintenance_status_on(
            connection: sqlite3.Connection,execution_id: str,*,owner: str
    ) -> MaintenanceExecutionStatus:
        from rquant.backfill_execute_contracts import MaintenanceExecutionStatus
        row=connection.execute(
            'SELECT owner,manifest_id,status,status_json FROM data_center_maintenance_execution '
            'WHERE execution_id=?',
            (execution_id,)).fetchone()
        if row is None or row['owner']!=owner:
            raise ValueError('owned maintenance execution is unavailable')
        value=MaintenanceExecutionStatus.model_validate_json(row['status_json'])
        if (value.status!=row['status'] or value.manifest_id!=row['manifest_id']
                or value.owner!=owner or value.execution_id!=execution_id):
            raise ValueError('maintenance status binding changed')
        counts=connection.execute(
            'SELECT COUNT(*) AS total,SUM(status=\'succeeded\') AS completed '
            'FROM backfill_task WHERE manifest_id=?',
            (value.manifest_id,)).fetchone()
        if value.total_tasks!=counts['total']:
            raise ValueError('maintenance task manifest count changed')
        return value.model_copy(update={'completed_tasks':int(counts['completed'] or 0)})

    def get_maintenance_status(self,execution_id: str,*,owner: str) -> MaintenanceExecutionStatus:
        self._require_maintenance()
        connection=self._connect()
        try:
            return self._maintenance_status_on(connection,execution_id,owner=owner)
        finally:
            connection.close()

    def get_backfill_execution_spec(
            self,execution_id: str,*,owner: str
    ) -> BackfillExecutionSpec | None:
        from rquant.backfill_execute_contracts import BackfillExecutionSpec
        self._require_maintenance()
        connection=self._connect()
        try:
            row=connection.execute(
                'SELECT owner,spec_json,spec_hash FROM data_center_maintenance_execution '
                'WHERE execution_id=?',
                (execution_id,)).fetchone()
            if row is None:
                return None
            if row['owner']!=owner or len(row['spec_json'].encode())>8_000_000:
                raise ValueError('owned execution source is unavailable')
            spec=BackfillExecutionSpec.model_validate_json(row['spec_json'])
            if self._content_hash(spec.model_dump(mode='json'))!=row['spec_hash']:
                raise ValueError('immutable maintenance execution content changed')
            return spec
        finally:
            connection.close()

    def admit_backfill_execution(self,spec: BackfillExecutionSpec,manifest: BackfillManifestInput,*,
                                now: datetime | None = None) -> MaintenanceExecutionStatus:
        from rquant.backfill_execute_contracts import (
            BackfillExecutionSpec,
            MaintenanceExecutionStatus,
        )
        self._require_maintenance()
        spec=BackfillExecutionSpec.model_validate_json(spec.model_dump_json())
        observed=_normalize_time(now or _utc_now())
        if (manifest.manifest_id!=spec.manifest_id
                or manifest.payload.get('execution_id')!=spec.execution_id):
            raise ValueError('original manifest differs from immutable execution')
        payload=spec.model_dump_json()
        with self._write_transaction() as connection:
            previous=connection.execute(
                'SELECT owner,spec_json FROM data_center_maintenance_execution '
                'WHERE execution_id=?',
                (spec.execution_id,)).fetchone()
            if previous is not None:
                if previous['owner']!=spec.owner or previous['spec_json']!=payload:
                    raise ValueError('same execution identity has different content')
                return self._maintenance_status_on(connection,spec.execution_id,owner=spec.owner)
            intent=connection.execute(
                'SELECT payload_json,consumed_command_id FROM data_center_maintenance_intent '
                'WHERE intent_id=?',
                (spec.intent.intent_id,)).fetchone()
            if (intent is None or intent['payload_json']!=spec.intent.model_dump_json()
                    or intent['consumed_command_id'] is not None
                    or not spec.intent.issued_at<=observed<spec.intent.expires_at):
                raise ValueError('execution confirmation is missing, consumed or expired')
            if connection.execute(
                    'SELECT COUNT(*) FROM data_center_maintenance_execution').fetchone()[0]>=4096:
                raise ValueError('maintenance execution history capacity reached')
            primary=spec.intent.primary_identity.generation_id
            occupied=connection.execute(
                'SELECT execution_id FROM data_center_maintenance_execution '
                'WHERE primary_generation=?\n'
                "                AND status IN ('queued','running','verifying')",
                (primary,)).fetchone()
            if occupied is not None:
                raise ValueError('another maintenance execution owns the physical primary')
            self._persist_manifest_on(connection,manifest,_encode_time(observed),
                self._content_hash(self._canonical_manifest(manifest)))
            status=MaintenanceExecutionStatus(execution_id=spec.execution_id,owner=spec.owner,kind='backfill',
                manifest_id=spec.manifest_id,plan_task_id=spec.plan_task_id,plan_sha256=spec.plan.content_sha256,
                source_generation_id=spec.intent.source_generation_id,policy_generation=spec.intent.policy_generation,
                status='queued',control_sequence=1,total_tasks=len(manifest.tasks),completed_tasks=0,
                created_at=observed,updated_at=observed)
            connection.execute('''INSERT INTO data_center_maintenance_execution
                (execution_id,owner,manifest_id,primary_generation,spec_json,spec_hash,status,status_json)
                VALUES (?,?,?,?,?,?,?,?)''',
                (spec.execution_id,spec.owner,spec.manifest_id,primary,payload,
                self._content_hash(spec.model_dump(mode='json')),
                status.status,status.model_dump_json()))
            connection.execute(
                'UPDATE data_center_maintenance_intent SET consumed_command_id=? WHERE intent_id=?',
                (spec.execute_command_id,spec.intent.intent_id))
            return status

    def get_financial_execution_spec(
            self,execution_id: str,*,owner: str
    ) -> FinancialExecutionSpec | None:
        from rquant.financial_runtime import FinancialExecutionSpec
        self._require_maintenance()
        connection=self._connect()
        try:
            row=connection.execute(
                'SELECT owner,spec_json,spec_hash FROM data_center_maintenance_execution '
                'WHERE execution_id=?',
                (execution_id,)).fetchone()
            if row is None:
                return None
            if row['owner']!=owner or len(row['spec_json'].encode())>8_000_000:
                raise ValueError('owned financial execution source is unavailable')
            spec=FinancialExecutionSpec.model_validate_json(row['spec_json'])
            if self._content_hash(spec.model_dump(mode='json'))!=row['spec_hash']:
                raise ValueError('immutable financial execution content changed')
            return spec
        finally:
            connection.close()

    def admit_financial_execution(
            self,spec: FinancialExecutionSpec,manifest: BackfillManifestInput,*,
            now: datetime | None = None) -> MaintenanceExecutionStatus:
        from rquant.backfill_execute_contracts import MaintenanceExecutionStatus
        from rquant.financial_runtime import FinancialExecutionSpec, build_financial_manifest
        self._require_maintenance()
        spec=FinancialExecutionSpec.model_validate_json(spec.model_dump_json())
        observed=_normalize_time(now or _utc_now())
        if (manifest!=build_financial_manifest(spec.plan)
                or len(spec.model_dump_json().encode())>8_000_000):
            raise ValueError('original financial manifest differs or exceeds capacity')
        payload=spec.model_dump_json()
        with self._write_transaction() as connection:
            previous=connection.execute(
                'SELECT owner,spec_json FROM data_center_maintenance_execution '
                'WHERE execution_id=?',
                (spec.execution_id,)).fetchone()
            if previous is not None:
                if previous['owner']!=spec.owner or previous['spec_json']!=payload:
                    raise ValueError('same financial execution identity has different content')
                return self._maintenance_status_on(connection,spec.execution_id,owner=spec.owner)
            intent=connection.execute(
                'SELECT payload_json,consumed_command_id FROM data_center_maintenance_intent '
                'WHERE intent_id=?',
                (spec.intent.intent_id,)).fetchone()
            if (intent is None or intent['payload_json']!=spec.intent.model_dump_json()
                    or intent['consumed_command_id'] is not None
                    or not spec.intent.issued_at<=observed<spec.intent.expires_at):
                raise ValueError('financial confirmation is missing, consumed or expired')
            if connection.execute(
                    'SELECT COUNT(*) FROM data_center_maintenance_execution').fetchone()[0]>=4096:
                raise ValueError('maintenance execution history capacity reached')
            primary=spec.plan.primary_identity.generation_id
            if connection.execute(
                    'SELECT 1 FROM data_center_maintenance_execution WHERE primary_generation=? '
                    "AND status IN ('queued','running','verifying')",
                    (primary,)).fetchone() is not None:
                raise ValueError('another maintenance execution owns the physical primary')
            self._persist_manifest_on(connection,manifest,_encode_time(observed),
                self._content_hash(self._canonical_manifest(manifest)))
            status=MaintenanceExecutionStatus(
                execution_id=spec.execution_id,owner=spec.owner,kind='financial',
                manifest_id=spec.manifest_id,
                plan_sha256=spec.plan.content_sha256,source_generation_id=spec.plan.source_generation_id,
                policy_generation=spec.admission_policy.policy_generation,status='queued',control_sequence=1,
                total_tasks=len(manifest.tasks),completed_tasks=0,created_at=observed,updated_at=observed)
            connection.execute('''INSERT INTO data_center_maintenance_execution
                (execution_id,owner,manifest_id,primary_generation,spec_json,spec_hash,status,status_json)
                VALUES (?,?,?,?,?,?,?,?)''',
                (spec.execution_id,spec.owner,spec.manifest_id,primary,payload,
                self._content_hash(spec.model_dump(mode='json')),
                status.status,status.model_dump_json()))
            connection.execute(
                'UPDATE data_center_maintenance_intent SET consumed_command_id=? WHERE intent_id=?',
                (spec.execute_command_id,spec.intent.intent_id))
            return status

    def maintenance_control(
            self,execution_id: str,*,owner: str,command_id: str,expected_sequence: int,
            action: Literal['pause','resume'],now: datetime | None = None,
            policy_generation: str | None = None) -> MaintenanceExecutionStatus:
        from rquant.backfill_execute_contracts import MaintenanceExecutionStatus
        self._require_maintenance()
        observed=_normalize_time(now or _utc_now())
        payload=_json_dumps({'execution_id':execution_id,'owner':owner,
            'expected_sequence':expected_sequence,'action':action})
        with self._write_transaction() as connection:
            previous=connection.execute(
                'SELECT payload_json,result_json FROM data_center_maintenance_control '
                'WHERE command_id=?',
                (command_id,)).fetchone()
            if previous is not None:
                if previous['payload_json']!=payload:
                    raise ValueError('same maintenance control command has different content')
                return MaintenanceExecutionStatus.model_validate_json(previous['result_json'])
            current=self._maintenance_status_on(connection,execution_id,owner=owner)
            if (current.control_sequence!=expected_sequence
                    or current.status in {'completed','failed'}):
                raise ValueError('maintenance control version is stale or terminal')
            changes={'control_sequence':expected_sequence+1,'updated_at':observed}
            if action=='pause':
                changes.update(pause_requested=True,pause_applied=current.status=='paused')
            elif action=='resume':
                if current.status not in {'paused','partial'}:
                    raise ValueError('only a released maintenance execution may resume')
                changes.update(status='queued',pause_requested=False,pause_applied=False,failure_code=None)
                if policy_generation is not None:
                    changes['policy_generation']=policy_generation
            else:
                raise ValueError('unknown maintenance control action')
            updated=MaintenanceExecutionStatus.model_validate(current.model_dump(mode='python')|changes)
            try:
                connection.execute(
                    'UPDATE data_center_maintenance_execution SET status=?,status_json=? '
                    'WHERE execution_id=?',
                    (updated.status,updated.model_dump_json(),execution_id))
            except sqlite3.IntegrityError as error:
                raise ValueError(
                    'another maintenance execution owns the physical primary') from error
            if connection.execute(
                    'SELECT COUNT(*) FROM data_center_maintenance_control').fetchone()[0]>=8192:
                raise ValueError('maintenance control history capacity reached')
            connection.execute(
                'INSERT INTO data_center_maintenance_control'
                '(command_id,execution_id,payload_json,result_json) VALUES (?,?,?,?)',
                (command_id,execution_id,payload,updated.model_dump_json()))
            return updated

    def maintenance_control_by_command(self,command_id: str,*,execution_id: str,owner: str,
            expected_sequence: int,action: Literal['pause','resume']
    ) -> MaintenanceExecutionStatus | None:
        from rquant.backfill_execute_contracts import MaintenanceExecutionStatus
        self._require_maintenance()
        expected=_json_dumps({'execution_id':execution_id,'owner':owner,
            'expected_sequence':expected_sequence,'action':action})
        connection=self._connect()
        try:
            row=connection.execute(
                'SELECT payload_json,result_json FROM data_center_maintenance_control '
                'WHERE command_id=?',(command_id,)).fetchone()
            if row is None:
                return None
            if row['payload_json']!=expected:
                raise ValueError('same maintenance command has different content or owner')
            return MaintenanceExecutionStatus.model_validate_json(row['result_json'])
        finally:
            connection.close()

    def transition_maintenance(self,execution_id: str,*,owner: str,expected_sequence: int,
            status: Literal['running','paused','partial','verifying','failed','completed'],
            failure_code: str | None = None,current_date: date | None = None,
            completion_sha256: str | None = None,audit_task_id: str | None = None,
            audit_report_sha256: str | None = None,
            now: datetime | None = None,connection: sqlite3.Connection | None = None
    ) -> MaintenanceExecutionStatus:
        from rquant.backfill_execute_contracts import MaintenanceExecutionStatus
        self._require_maintenance()
        if connection is None:
            with self._write_transaction() as opened:
                return self.transition_maintenance(
                    execution_id,owner=owner,expected_sequence=expected_sequence,status=status,
                    failure_code=failure_code,current_date=current_date,
                    completion_sha256=completion_sha256,audit_task_id=audit_task_id,
                    audit_report_sha256=audit_report_sha256,now=now,connection=opened)
        if not connection.in_transaction:
            raise ValueError('maintenance transition requires original state transaction')
        current=self._maintenance_status_on(connection,execution_id,owner=owner)
        if current.control_sequence!=expected_sequence or current.status in {'completed','failed'}:
            raise ValueError('maintenance transition is stale or terminal')
        if status in {'paused','partial','failed','completed'}:
            running=connection.execute(
                "SELECT COUNT(*) FROM backfill_task WHERE manifest_id=? AND status='running'",
                (current.manifest_id,)).fetchone()[0]
            if running:
                raise ValueError(
                    'active original claim must be released before releasing maintenance slot')
        if status=='completed' and current.completed_tasks!=current.total_tasks:
            raise ValueError('unfinished original tasks cannot complete maintenance')
        updated=MaintenanceExecutionStatus.model_validate(current.model_dump(mode='python')|{
            'status':status,'control_sequence':expected_sequence+1,'pause_applied':status=='paused',
            'updated_at':_normalize_time(now or _utc_now()),'failure_code':failure_code,
            'current_date':current_date,'completion_sha256':completion_sha256,
            'audit_task_id':audit_task_id or current.audit_task_id,
            'audit_report_sha256':audit_report_sha256 or current.audit_report_sha256})
        connection.execute(
            'UPDATE data_center_maintenance_execution SET status=?,status_json=? '
            'WHERE execution_id=?',
            (updated.status,updated.model_dump_json(),execution_id))
        return updated

    def verify_maintenance_claim(self,connection: sqlite3.Connection,claim: ClaimedBackfillTask,*,
            execution_id: str,owner: str,expected_sequence: int,now: datetime
    ) -> MaintenanceExecutionStatus:
        self._require_maintenance()
        current=self._maintenance_status_on(connection,execution_id,owner=owner)
        if (current.manifest_id!=claim.manifest_id or current.control_sequence!=expected_sequence
                or current.status!='running' or current.pause_requested or current.pause_applied):
            raise StaleTaskClaimError('maintenance slot or control sequence changed')
        self._require_manifest_active(connection,claim.manifest_id)
        self._require_current_claim(connection,claim)
        row=connection.execute(
            'SELECT lease_expires_at FROM backfill_task WHERE manifest_id=? AND task_id=?',
            (claim.manifest_id,claim.task_id)).fetchone()
        if row is None or _decode_time(row['lease_expires_at'])<=_normalize_time(now):
            raise StaleTaskClaimError('original maintenance claim lease expired')
        return current

    def load_manifest(self, manifest_id: str) -> BackfillManifestInput | None:
        connection = self._connect()
        try:
            manifest_row = connection.execute(
                """
                SELECT payload_json, content_hash
                FROM backfill_manifest
                WHERE manifest_id = ?
                """,
                (manifest_id,),
            ).fetchone()
            if manifest_row is None:
                return None
            task_rows = connection.execute(
                """
                SELECT task_id, payload_json, max_attempts, content_hash
                FROM backfill_task
                WHERE manifest_id = ?
                ORDER BY ordinal
                """,
                (manifest_id,),
            ).fetchall()
            eligibility_rows = connection.execute(
                """
                SELECT eligibility_id, payload_json, content_hash
                FROM backfill_eligibility
                WHERE manifest_id = ?
                ORDER BY ordinal
                """,
                (manifest_id,),
            ).fetchall()
        finally:
            connection.close()
        loaded = BackfillManifestInput(
            manifest_id=manifest_id,
            payload=_json_object(manifest_row["payload_json"]),
            tasks=tuple(
                BackfillTaskInput(
                    task_id=row["task_id"],
                    payload=_json_object(row["payload_json"]),
                    max_attempts=row["max_attempts"],
                )
                for row in task_rows
            ),
            eligibility=tuple(
                BackfillEligibilityInput(
                    eligibility_id=row["eligibility_id"],
                    payload=_json_object(row["payload_json"]),
                )
                for row in eligibility_rows
            ),
        )
        for task, row in zip(loaded.tasks, task_rows, strict=True):
            expected_hash = self._content_hash(task.model_dump(mode="json"))
            if row["content_hash"] != expected_hash:
                raise ManifestContentConflictError(
                    f"manifest {manifest_id!r} task {task.task_id!r} "
                    "content hash mismatch"
                )
        for eligibility, row in zip(
            loaded.eligibility,
            eligibility_rows,
            strict=True,
        ):
            expected_hash = self._content_hash(
                eligibility.model_dump(mode="json")
            )
            if row["content_hash"] != expected_hash:
                raise ManifestContentConflictError(
                    f"manifest {manifest_id!r} eligibility "
                    f"{eligibility.eligibility_id!r} content hash mismatch"
                )
        expected_manifest_hash = self._content_hash(
            self._canonical_manifest(loaded)
        )
        if manifest_row["content_hash"] != expected_manifest_hash:
            raise ManifestContentConflictError(
                f"manifest {manifest_id!r} content hash mismatch"
            )
        return loaded

    def _require_manifest(self, connection: sqlite3.Connection, manifest_id: str) -> None:
        row = connection.execute(
            "SELECT 1 FROM backfill_manifest WHERE manifest_id = ?",
            (manifest_id,),
        ).fetchone()
        if row is None:
            raise UnknownManifestError(f"unknown backfill manifest {manifest_id!r}")

    @staticmethod
    def _require_manifest_active(
        connection: sqlite3.Connection,
        manifest_id: str,
    ) -> None:
        row = connection.execute(
            "SELECT terminal_status FROM backfill_manifest WHERE manifest_id = ?",
            (manifest_id,),
        ).fetchone()
        if row is None:
            raise UnknownManifestError(f"unknown backfill manifest {manifest_id!r}")
        if row["terminal_status"] == "abandoned":
            raise ManifestAbandonedError(
                f"backfill manifest {manifest_id!r} was abandoned"
            )

    @staticmethod
    def _failure_from_json(value: str | None) -> BackfillFailure | None:
        if value is None:
            return None
        return BackfillFailure.model_validate_json(value)

    @staticmethod
    def _metrics_from_json(value: str | None) -> BackfillTaskMetrics:
        if value is None:
            return BackfillTaskMetrics()
        return BackfillTaskMetrics.model_validate_json(value)

    @staticmethod
    def _accumulate_metrics(
        previous: BackfillTaskMetrics,
        current: BackfillTaskMetrics,
    ) -> BackfillTaskMetrics:
        return BackfillTaskMetrics(
            request_count=previous.request_count + current.request_count,
            returned_rows=previous.returned_rows + current.returned_rows,
            written_rows=previous.written_rows + current.written_rows,
            covered_sessions=max(
                previous.covered_sessions,
                current.covered_sessions,
            ),
            allowed_missing_sessions=max(
                previous.allowed_missing_sessions,
                current.allowed_missing_sessions,
            ),
        )

    def claim_task(
        self,
        manifest_id: str,
        *,
        worker_id: str,
        lease_seconds: int,
        retry_failed: bool = False,
        exclude_task_ids: set[str] | None = None,
        after_ordinal: int = -1,
        now: datetime | None = None,
        maintenance_execution_id: str | None = None,
    ) -> ClaimedBackfillTask | None:
        if not worker_id.strip():
            raise ValueError("worker_id must not be blank")
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        if after_ordinal < -1:
            raise ValueError("after_ordinal must be at least -1")
        claimed_at = _normalize_time(now or _utc_now())
        lease_expires_at = claimed_at + timedelta(seconds=lease_seconds)
        with self._write_transaction() as connection:
            self._require_manifest_active(connection, manifest_id)
            metadata_present=connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='data_center_maintenance_execution'").fetchone()
            controlled=None if metadata_present is None else connection.execute(
                'SELECT execution_id,status,status_json FROM data_center_maintenance_execution '
                'WHERE manifest_id=?',(manifest_id,)).fetchone()
            if controlled is not None:
                from rquant.backfill_execute_contracts import MaintenanceExecutionStatus
                control=MaintenanceExecutionStatus.model_validate_json(controlled['status_json'])
                if (controlled['execution_id']!=maintenance_execution_id
                        or controlled['status']!='running'
                        or control.pause_requested or control.pause_applied):
                    raise StaleTaskClaimError(
                        'controlled manifest requires its current maintenance slot')
                occupied=connection.execute(
                    "SELECT 1 FROM backfill_task WHERE manifest_id=? AND status='running' "
                    'AND lease_expires_at>? LIMIT 1',
                    (manifest_id,_encode_time(claimed_at))).fetchone()
                if occupied is not None:
                    return None
            elif maintenance_execution_id is not None:
                raise StaleTaskClaimError('maintenance claim is not registered in original state')
            candidate: sqlite3.Row | None = None
            recovery_only = False
            while True:
                predicates = [
                    "manifest_id = ?",
                    "ordinal > ?",
                    "("
                    "status = 'pending' OR "
                    "(status = 'running' AND lease_expires_at IS NOT NULL "
                    "AND lease_expires_at <= ?) OR "
                    "(? = 1 AND status = 'failed' AND attempts < max_attempts "
                    "AND COALESCE(json_extract(failure_json, '$.retryable'), 0) = 1)"
                    ")",
                ]
                parameters: list[object] = [
                    manifest_id,
                    after_ordinal,
                    _encode_time(claimed_at),
                    int(retry_failed),
                ]
                if exclude_task_ids:
                    placeholders = ", ".join("?" for _ in exclude_task_ids)
                    predicates.append(f"task_id NOT IN ({placeholders})")
                    parameters.extend(sorted(exclude_task_ids))
                row = connection.execute(
                    f"""
                    SELECT task_id, ordinal, payload_json, status, attempts,
                           max_attempts, lease_expires_at, failure_json,
                           recovery_attempted
                    FROM backfill_task
                    WHERE {' AND '.join(predicates)}
                    ORDER BY ordinal
                    LIMIT 1
                    """,
                    parameters,
                ).fetchone()
                if row is None:
                    break
                status = cast(BackfillTaskStatus, row["status"])
                attempts = int(row["attempts"])
                max_attempts = int(row["max_attempts"])
                if status == "pending":
                    candidate = row
                    recovery_only = attempts >= max_attempts
                    break
                if status == "running":
                    expiry = _decode_time(row["lease_expires_at"])
                    if expiry is None or expiry > claimed_at:
                        continue
                    if attempts < max_attempts:
                        candidate = row
                        break
                    if int(row["recovery_attempted"]) == 0:
                        candidate = row
                        recovery_only = True
                        break
                    exhausted = BackfillFailure(
                        code="lease_expired",
                        message="task lease expired after the final allowed attempt",
                        retryable=False,
                        details={"attempts": attempts},
                    )
                    connection.execute(
                        """
                        UPDATE backfill_task
                        SET status = 'failed', worker_id = NULL, claim_token = NULL,
                            claimed_at = NULL, lease_seconds = NULL,
                            lease_expires_at = NULL, failure_json = ?, finished_at = ?
                        WHERE manifest_id = ? AND task_id = ?
                        """,
                        (
                            exhausted.model_dump_json(),
                            _encode_time(claimed_at),
                            manifest_id,
                            row["task_id"],
                        ),
                    )
                    continue
                if status == "failed" and retry_failed and attempts < max_attempts:
                    failure = self._failure_from_json(row["failure_json"])
                    if failure is not None and failure.retryable:
                        candidate = row
                        break
            if candidate is None:
                return None

            attempt = (
                int(candidate["attempts"])
                if recovery_only
                else int(candidate["attempts"]) + 1
            )
            claim_token = uuid4().hex
            connection.execute(
                """
                UPDATE backfill_task
                SET status = 'running', attempts = ?, worker_id = ?, claim_token = ?,
                    claimed_at = ?, lease_seconds = ?, lease_expires_at = ?,
                    failure_json = NULL, recovery_attempted = ?,
                    duration_seconds = NULL, finished_at = NULL
                WHERE manifest_id = ? AND task_id = ?
                """,
                (
                    attempt,
                    worker_id.strip(),
                    claim_token,
                    _encode_time(claimed_at),
                    lease_seconds,
                    _encode_time(lease_expires_at),
                    1 if recovery_only else int(candidate["recovery_attempted"]),
                    manifest_id,
                    candidate["task_id"],
                ),
            )
            connection.execute(
                "UPDATE backfill_manifest SET updated_at = ? WHERE manifest_id = ?",
                (_encode_time(claimed_at), manifest_id),
            )
            return ClaimedBackfillTask(
                manifest_id=manifest_id,
                task_id=candidate["task_id"],
                ordinal=int(candidate["ordinal"]),
                payload=_json_object(candidate["payload_json"]),
                attempt=attempt,
                max_attempts=candidate["max_attempts"],
                worker_id=worker_id.strip(),
                claim_token=claim_token,
                recovery_only=recovery_only,
                claimed_at=claimed_at,
                lease_expires_at=lease_expires_at,
            )

    @staticmethod
    def _require_current_claim(
        connection: sqlite3.Connection,
        claim: ClaimedBackfillTask,
    ) -> sqlite3.Row:
        row = connection.execute(
            """
            SELECT status, claim_token, metrics_json, recovery_attempted
            FROM backfill_task
            WHERE manifest_id = ? AND task_id = ?
            """,
            (claim.manifest_id, claim.task_id),
        ).fetchone()
        if row is None:
            raise UnknownTaskError(f"unknown backfill task {claim.task_id!r}")
        if row["status"] != "running" or row["claim_token"] != claim.claim_token:
            raise StaleTaskClaimError(
                f"backfill task {claim.task_id!r} is no longer owned by this claim"
            )
        return row

    def renew_task_claim(
        self,
        claim: ClaimedBackfillTask,
        *,
        lease_seconds: int,
        now: datetime | None = None,
        guard: Callable[[sqlite3.Connection,datetime],None] | None = None,
    ) -> ClaimedBackfillTask:
        """Fence a slow worker before it writes by atomically extending ownership."""
        if lease_seconds < 1:
            raise ValueError("lease_seconds must be positive")
        renewed_at = _normalize_time(now or _utc_now())
        lease_expires_at = renewed_at + timedelta(seconds=lease_seconds)
        with self._write_transaction() as connection:
            self._require_current_claim(connection, claim)
            if guard is not None:
                guard(connection,renewed_at)
            connection.execute(
                """
                UPDATE backfill_task
                SET lease_seconds = ?, lease_expires_at = ?
                WHERE manifest_id = ? AND task_id = ?
                """,
                (
                    lease_seconds,
                    _encode_time(lease_expires_at),
                    claim.manifest_id,
                    claim.task_id,
                ),
            )
            connection.execute(
                "UPDATE backfill_manifest SET updated_at = ? WHERE manifest_id = ?",
                (_encode_time(renewed_at), claim.manifest_id),
            )
        return claim.model_copy(update={"lease_expires_at": lease_expires_at})

    def release_task_claim(
        self,
        claim: ClaimedBackfillTask,
        *,
        now: datetime | None = None,
    ) -> None:
        """Return an interrupted claim to pending without spending an attempt."""
        with self._write_transaction() as connection:
            self._release_claim_on(connection,claim,now=now)

    def _release_claim_on(
            self,connection: sqlite3.Connection,claim: ClaimedBackfillTask,*,now: datetime | None
    ) -> None:
        released_at = _normalize_time(now or _utc_now())
        attempts = claim.attempt if claim.recovery_only else max(claim.attempt - 1, 0)
        task_row = self._require_current_claim(connection, claim)
        recovery_attempted = 0 if claim.recovery_only else int(task_row['recovery_attempted'])
        connection.execute(
                """
                UPDATE backfill_task
                SET status = 'pending', attempts = ?, recovery_attempted = ?,
                    worker_id = NULL, claim_token = NULL, claimed_at = NULL,
                    lease_seconds = NULL, lease_expires_at = NULL,
                    failure_json = NULL, metrics_json = NULL,
                    duration_seconds = NULL, finished_at = NULL
                WHERE manifest_id = ? AND task_id = ?
                """,
                (
                    attempts,
                    recovery_attempted,
                    claim.manifest_id,
                    claim.task_id,
                ),
            )
        connection.execute(
                "UPDATE backfill_manifest SET updated_at = ? WHERE manifest_id = ?",
                (_encode_time(released_at), claim.manifest_id),
            )

    def release_expired_maintenance_claims(
            self,execution_id: str,*,owner: str,expected_sequence: int,
            now: datetime | None = None) -> int:
        self._require_maintenance()
        observed=_normalize_time(now or _utc_now())
        with self._write_transaction() as connection:
            current=self._maintenance_status_on(connection,execution_id,owner=owner)
            if (current.control_sequence!=expected_sequence
                    or current.status not in {'queued','running','verifying'}):
                raise StaleTaskClaimError('maintenance pause control changed')
            self._require_manifest_active(connection,current.manifest_id)
            rows=connection.execute(
                "SELECT * FROM backfill_task WHERE manifest_id=? AND status='running' "
                'AND lease_expires_at<=? ORDER BY ordinal LIMIT 4097',
                (current.manifest_id,_encode_time(observed))).fetchall()
            if len(rows)>4096:
                raise ValueError('original maintenance task scope exceeds frozen capacity')
            for row in rows:
                claim=ClaimedBackfillTask(
                    manifest_id=current.manifest_id,task_id=row['task_id'],ordinal=row['ordinal'],
                    payload=_json_object(row['payload_json']),attempt=row['attempts'],
                    max_attempts=row['max_attempts'],worker_id=row['worker_id'],
                    claim_token=row['claim_token'],claimed_at=_decode_time(row['claimed_at']),
                    lease_expires_at=_decode_time(row['lease_expires_at']),
                    recovery_only=bool(row['recovery_attempted'])
                    and row['attempts']>=row['max_attempts'])
                self._release_claim_on(connection,claim,now=observed)
            return len(rows)

    def mark_task_succeeded(
        self,
        claim: ClaimedBackfillTask,
        *,
        duration_seconds: float,
        metrics: BackfillTaskMetrics | None = None,
        now: datetime | None = None,
    ) -> None:
        with self._write_transaction() as connection:
            self._succeed_in_transaction(connection,claim,duration_seconds=duration_seconds,
                                         metrics=metrics,now=now)

    def _succeed_in_transaction(
        self, connection: sqlite3.Connection, claim: ClaimedBackfillTask, *,
        duration_seconds: float, metrics: BackfillTaskMetrics | None = None,
        now: datetime | None = None,
    ) -> None:
        if duration_seconds < 0 or not math.isfinite(duration_seconds):
            raise ValueError("duration_seconds must be finite and non-negative")
        finished_at = _normalize_time(now or _utc_now())
        task_row = self._require_current_claim(connection,claim)
        resolved_metrics = self._accumulate_metrics(
            self._metrics_from_json(task_row['metrics_json']),metrics or BackfillTaskMetrics(),
        )
        manifest = connection.execute(
            'SELECT ewma_duration_seconds FROM backfill_manifest WHERE manifest_id = ?',
            (claim.manifest_id,),
        ).fetchone()
        if manifest is None:
            raise UnknownManifestError(f"unknown backfill manifest {claim.manifest_id!r}")
        previous = manifest['ewma_duration_seconds']
        ewma = duration_seconds if previous is None else (
            self.ewma_alpha*duration_seconds+(1-self.ewma_alpha)*float(previous)
        )
        connection.execute('''
            UPDATE backfill_task SET status='succeeded', worker_id=NULL, claim_token=NULL,
                claimed_at=NULL, lease_seconds=NULL, lease_expires_at=NULL,
                failure_json=NULL, metrics_json=?, duration_seconds=?, finished_at=?
            WHERE manifest_id=? AND task_id=?
        ''',(resolved_metrics.model_dump_json(),duration_seconds,_encode_time(finished_at),
             claim.manifest_id,claim.task_id))
        connection.execute('''
            UPDATE backfill_manifest SET ewma_duration_seconds=?, updated_at=?
            WHERE manifest_id=?
        ''',(ewma,_encode_time(finished_at),claim.manifest_id))

    @contextmanager
    def commit_claim(
        self, claim: ClaimedBackfillTask, *, now: datetime | None = None,
        lease_seconds: int = 120,
        guard: Callable[[sqlite3.Connection,datetime],None] | None = None,
    ) -> Iterator[ProtectedBackfillClaim]:
        """Keep the original state writer exclusion across the caller's fact COMMIT.

        The caller must acquire its primary gate first and do all network work outside.
        This is the original SQLite file/transaction, not a second claim authority.
        """
        if lease_seconds < 1 or self.busy_timeout_ms > 5_000:
            raise ValueError('protected commit requires positive lease and <=5s SQLite busy')
        observed = _normalize_time(now or _utc_now())
        with self._write_transaction() as connection:
            protected = ProtectedBackfillClaim(self,connection,claim,guard)
            protected.verify(now=observed)
            connection.execute('''
                UPDATE backfill_task SET lease_seconds=?, lease_expires_at=?
                WHERE manifest_id=? AND task_id=?
            ''',(lease_seconds,_encode_time(observed+timedelta(seconds=lease_seconds)),
                 claim.manifest_id,claim.task_id))
            yield protected

    def mark_task_failed(
        self,
        claim: ClaimedBackfillTask,
        *,
        failure: BackfillFailure,
        metrics: BackfillTaskMetrics | None = None,
        now: datetime | None = None,
    ) -> None:
        finished_at = _normalize_time(now or _utc_now())
        current_metrics = metrics or BackfillTaskMetrics()
        with self._write_transaction() as connection:
            task_row = self._require_current_claim(connection, claim)
            resolved_metrics = self._accumulate_metrics(
                self._metrics_from_json(task_row["metrics_json"]),
                current_metrics,
            )
            connection.execute(
                """
                UPDATE backfill_task
                SET status = 'failed', worker_id = NULL, claim_token = NULL,
                    claimed_at = NULL, lease_seconds = NULL, lease_expires_at = NULL,
                    failure_json = ?, metrics_json = ?, finished_at = ?
                WHERE manifest_id = ? AND task_id = ?
                """,
                (
                    failure.model_dump_json(),
                    resolved_metrics.model_dump_json(),
                    _encode_time(finished_at),
                    claim.manifest_id,
                    claim.task_id,
                ),
            )
            connection.execute(
                "UPDATE backfill_manifest SET updated_at = ? WHERE manifest_id = ?",
                (_encode_time(finished_at), claim.manifest_id),
            )

    def get_task(self, manifest_id: str, task_id: str) -> BackfillTaskState:
        connection = self._connect()
        try:
            row = connection.execute(
                """
                SELECT * FROM backfill_task
                WHERE manifest_id = ? AND task_id = ?
                """,
                (manifest_id, task_id),
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            raise UnknownTaskError(f"unknown backfill task {task_id!r}")
        return BackfillTaskState(
            manifest_id=manifest_id,
            task_id=task_id,
            payload=_json_object(row["payload_json"]),
            status=row["status"],
            attempts=row["attempts"],
            max_attempts=row["max_attempts"],
            worker_id=row["worker_id"],
            claim_token=row["claim_token"],
            claimed_at=_decode_time(row["claimed_at"]),
            lease_expires_at=_decode_time(row["lease_expires_at"]),
            failure=self._failure_from_json(row["failure_json"]),
            metrics=self._metrics_from_json(row["metrics_json"]),
            duration_seconds=row["duration_seconds"],
            finished_at=_decode_time(row["finished_at"]),
        )

    def get_manifest_status(self, manifest_id: str) -> BackfillManifestStatus:
        connection = self._connect()
        try:
            manifest_columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info(backfill_manifest)"
                ).fetchall()
            }
            termination_projection = ", ".join(
                column if column in manifest_columns else f"NULL AS {column}"
                for column in (
                    "terminal_status",
                    "termination_reason",
                    "terminated_at",
                    "terminated_by_commit",
                    "termination_plan_id",
                )
            )
            manifest = connection.execute(
                f"""
                SELECT content_hash, updated_at, ewma_duration_seconds,
                       {termination_projection}
                FROM backfill_manifest
                WHERE manifest_id = ?
                """,
                (manifest_id,),
            ).fetchone()
            if manifest is None:
                raise UnknownManifestError(f"unknown backfill manifest {manifest_id!r}")
            task_rows = connection.execute(
                """
                SELECT task_id, status, attempts, max_attempts,
                       failure_json, metrics_json
                FROM backfill_task
                WHERE manifest_id = ?
                ORDER BY ordinal
                """,
                (manifest_id,),
            ).fetchall()
            eligibility_count = connection.execute(
                """
                SELECT COUNT(*) FROM backfill_eligibility WHERE manifest_id = ?
                """,
                (manifest_id,),
            ).fetchone()[0]
        finally:
            connection.close()

        counts = {status: 0 for status in ("pending", "running", "succeeded", "failed")}
        failures: list[BackfillTaskFailureSummary] = []
        retryable_failed = 0
        metric_totals = {
            "request_count": 0,
            "returned_rows": 0,
            "written_rows": 0,
            "covered_sessions": 0,
            "allowed_missing_sessions": 0,
        }
        for row in task_rows:
            status = cast(BackfillTaskStatus, row["status"])
            counts[status] += 1
            failure = self._failure_from_json(row["failure_json"])
            metrics = self._metrics_from_json(row["metrics_json"])
            for field_name in metric_totals:
                metric_totals[field_name] += int(getattr(metrics, field_name))
            if status == "failed" and failure is not None:
                failures.append(
                    BackfillTaskFailureSummary(
                        task_id=row["task_id"],
                        attempts=row["attempts"],
                        max_attempts=row["max_attempts"],
                        failure=failure,
                    )
                )
                if failure.retryable and row["attempts"] < row["max_attempts"]:
                    retryable_failed += 1

        task_count = len(task_rows)
        termination = None
        if manifest["terminal_status"] == "abandoned":
            status_value: BackfillManifestStatusValue = "abandoned"
            terminal = True
            termination = BackfillManifestTermination(
                reason=manifest["termination_reason"],
                code_commit=manifest["terminated_by_commit"],
                terminated_at=_decode_time(manifest["terminated_at"]),
                plan_id=manifest["termination_plan_id"],
            )
        elif counts["succeeded"] == task_count:
            status_value: BackfillManifestStatusValue = "completed"
            terminal = True
        elif counts["pending"] == 0 and counts["running"] == 0:
            status_value = "failed"
            terminal = retryable_failed == 0
        elif counts["running"] or counts["succeeded"]:
            status_value = "running"
            terminal = False
        else:
            status_value = "pending"
            terminal = False

        remaining = counts["pending"] + counts["running"] + retryable_failed
        ewma = manifest["ewma_duration_seconds"]
        eta = None if ewma is None or terminal else float(ewma) * remaining
        return BackfillManifestStatus(
            manifest_id=manifest_id,
            status=status_value,
            terminal=terminal,
            task_count=task_count,
            eligibility_count=eligibility_count,
            pending=counts["pending"],
            running=counts["running"],
            succeeded=counts["succeeded"],
            failed=counts["failed"],
            ewma_duration_seconds=ewma,
            eta_seconds=eta,
            request_count=metric_totals["request_count"],
            returned_rows=metric_totals["returned_rows"],
            written_rows=metric_totals["written_rows"],
            covered_sessions=metric_totals["covered_sessions"],
            allowed_missing_sessions=metric_totals["allowed_missing_sessions"],
            failures=tuple(failures),
            termination=termination,
        )

    @staticmethod
    def _abandonment_plan_id(
        plan: BackfillManifestAbandonmentPlan,
    ) -> str:
        payload = plan.model_dump(mode="json", exclude={"plan_id"})
        return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()

    def plan_manifest_abandonment(
        self,
        manifest_id: str,
        *,
        reason: str,
        code_commit: str,
    ) -> BackfillManifestAbandonmentPlan:
        normalized_reason = reason.strip()
        if not normalized_reason:
            raise ValueError("manifest abandonment reason must not be blank")
        status = self.get_manifest_status(manifest_id)
        if status.terminal:
            raise ManifestAbandonmentConflictError(
                f"manifest {manifest_id!r} is already terminal: {status.status}"
            )
        if status.running:
            raise ManifestAbandonmentConflictError(
                f"manifest {manifest_id!r} has {status.running} running task(s)"
            )
        connection = self._connect()
        try:
            manifest = connection.execute(
                """
                SELECT content_hash, updated_at
                FROM backfill_manifest
                WHERE manifest_id = ?
                """,
                (manifest_id,),
            ).fetchone()
        finally:
            connection.close()
        if manifest is None:
            raise UnknownManifestError(f"unknown backfill manifest {manifest_id!r}")
        draft = BackfillManifestAbandonmentPlan(
            manifest_id=manifest_id,
            manifest_content_hash=manifest["content_hash"],
            manifest_updated_at=_decode_time(manifest["updated_at"]),
            status_before=status.status,
            task_count=status.task_count,
            pending=status.pending,
            running=status.running,
            succeeded=status.succeeded,
            failed=status.failed,
            reason=normalized_reason,
            code_commit=code_commit.strip(),
            plan_id="0" * 64,
        )
        return draft.model_copy(
            update={"plan_id": self._abandonment_plan_id(draft)}
        )

    def apply_manifest_abandonment(
        self,
        plan: BackfillManifestAbandonmentPlan,
        *,
        now: datetime | None = None,
    ) -> BackfillManifestStatus:
        expected_plan_id = self._abandonment_plan_id(plan)
        if plan.plan_id != expected_plan_id:
            raise StaleManifestAbandonmentError(
                f"manifest {plan.manifest_id!r} abandonment plan hash is invalid"
            )
        terminated_at = _normalize_time(now or _utc_now())
        with self._write_transaction() as connection:
            manifest = connection.execute(
                """
                SELECT content_hash, updated_at, terminal_status
                FROM backfill_manifest
                WHERE manifest_id = ?
                """,
                (plan.manifest_id,),
            ).fetchone()
            if manifest is None:
                raise UnknownManifestError(
                    f"unknown backfill manifest {plan.manifest_id!r}"
                )
            counts = {
                row["status"]: int(row["count"])
                for row in connection.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM backfill_task
                    WHERE manifest_id = ?
                    GROUP BY status
                    """,
                    (plan.manifest_id,),
                ).fetchall()
            }
            current_counts = {
                status: counts.get(status, 0)
                for status in ("pending", "running", "succeeded", "failed")
            }
            expected_counts = {
                "pending": plan.pending,
                "running": plan.running,
                "succeeded": plan.succeeded,
                "failed": plan.failed,
            }
            if current_counts["running"]:
                raise ManifestAbandonmentConflictError(
                    f"manifest {plan.manifest_id!r} has running task(s)"
                )
            if (
                manifest["terminal_status"] is not None
                or manifest["content_hash"] != plan.manifest_content_hash
                or _decode_time(manifest["updated_at"])
                != plan.manifest_updated_at
                or current_counts != expected_counts
            ):
                raise StaleManifestAbandonmentError(
                    f"manifest {plan.manifest_id!r} changed after abandonment plan"
                )
            connection.execute(
                """
                UPDATE backfill_manifest
                SET terminal_status = 'abandoned', termination_reason = ?,
                    terminated_at = ?, terminated_by_commit = ?,
                    termination_plan_id = ?, updated_at = ?
                WHERE manifest_id = ?
                  AND terminal_status IS NULL
                  AND content_hash = ?
                  AND updated_at = ?
                """,
                (
                    plan.reason,
                    _encode_time(terminated_at),
                    plan.code_commit,
                    plan.plan_id,
                    _encode_time(terminated_at),
                    plan.manifest_id,
                    plan.manifest_content_hash,
                    _encode_time(plan.manifest_updated_at),
                ),
            )
            if connection.execute("SELECT changes()").fetchone()[0] != 1:
                raise StaleManifestAbandonmentError(
                    f"manifest {plan.manifest_id!r} changed during abandonment"
                )
        return self.get_manifest_status(plan.manifest_id)

    def get_workload_telemetry(
        self,
        manifest_id: str,
        *,
        source: str,
        freq: str,
        response_row_limit: int,
        sample_limit: int = 128,
    ) -> BackfillWorkloadTelemetry:
        if not source.strip() or not freq.strip():
            raise ValueError("source and freq must not be blank")
        if response_row_limit < 1 or sample_limit < 1:
            raise ValueError("row and sample limits must be positive")
        connection = self._connect()
        try:
            self._require_manifest(connection, manifest_id)
            remaining = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COALESCE(SUM(
                        CAST(json_extract(payload_json, '$.expected_rows') AS INTEGER)
                    ), 0)
                FROM backfill_task
                WHERE manifest_id = ?
                  AND (
                      status IN ('pending', 'running')
                      OR (
                          status = 'failed'
                          AND attempts < max_attempts
                          AND COALESCE(
                              json_extract(failure_json, '$.retryable'),
                              0
                          ) = 1
                      )
                  )
                """,
                (manifest_id,),
            ).fetchone()
            samples = connection.execute(
                """
                SELECT manifest_id,
                       duration_seconds,
                       CAST(
                           json_extract(payload_json, '$.expected_rows')
                           AS REAL
                       ) AS expected_rows
                FROM backfill_task
                WHERE status = 'succeeded'
                  AND duration_seconds IS NOT NULL
                  AND duration_seconds >= 0
                  AND CAST(
                      json_extract(metrics_json, '$.request_count')
                      AS INTEGER
                  ) > 0
                  AND json_extract(payload_json, '$.source') = ?
                  AND json_extract(payload_json, '$.freq') = ?
                  AND CAST(
                      json_extract(payload_json, '$.response_row_limit')
                      AS INTEGER
                  ) = ?
                  AND CAST(
                      json_extract(payload_json, '$.expected_rows')
                      AS INTEGER
                  ) > 0
                ORDER BY finished_at DESC, manifest_id, ordinal
                LIMIT ?
                """,
                (
                    source.strip(),
                    freq.strip(),
                    response_row_limit,
                    sample_limit,
                ),
            ).fetchall()
        finally:
            connection.close()

        durations = [float(row["duration_seconds"]) for row in samples]
        seconds_per_row = [
            float(row["duration_seconds"]) / float(row["expected_rows"])
            for row in samples
        ]
        return BackfillWorkloadTelemetry(
            remaining_tasks=int(remaining[0]),
            remaining_expected_rows=int(remaining[1]),
            sample_task_count=len(samples),
            sample_manifest_count=len(
                {str(row["manifest_id"]) for row in samples}
            ),
            p75_task_seconds=_linear_quantile(durations, 0.75),
            p75_seconds_per_row=_linear_quantile(seconds_per_row, 0.75),
        )
