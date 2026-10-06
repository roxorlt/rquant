"""Global scheduling metadata under the original Lab DB and claim-spool lock."""

from __future__ import annotations

import fcntl
import hashlib
import os
import sqlite3
import stat
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import TYPE_CHECKING, Annotated, Iterator, Literal
from uuid import UUID, uuid4

from pydantic import Field, StrictBool, StrictInt, StrictStr, model_validator

from rquant.backtest.contracts import Sha256
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strict_json import canonical_json_bytes, canonical_model_json_bytes, strict_model_validate_canonical_json

if TYPE_CHECKING:
    from rquant.lab_claim_publication import LabClaimPublicationRecord
    from rquant.lab_jobs import LabJobReader, LabJobStore, LabLeaseRecord
    from rquant.lab_shard_protocol import LabSpoolClaim

_MAX = 2**63 - 1
_MARKER = ".scheduling-barrier-v1.json"
_STATE_SQL = """CREATE TABLE lab_scheduler_control (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 payload_json TEXT NOT NULL CHECK(length(CAST(payload_json AS BLOB))<=8192 AND json_valid(payload_json))
) STRICT"""
_RECEIPT_SQL = """CREATE TABLE lab_scheduler_control_receipt (
 request_id TEXT PRIMARY KEY,
 content_hash TEXT NOT NULL CHECK(length(content_hash)=64),
 payload_json TEXT NOT NULL CHECK(length(CAST(payload_json AS BLOB))<=8192 AND json_valid(payload_json))
) STRICT"""


class LabSchedulingQueueIdentity(RuntimeContractModel):
    canonical_job_store_path: StrictStr
    database_generation: tuple[StrictInt, StrictInt]
    store_id: Sha256
    application_id: StrictInt = Field(ge=1, le=_MAX)
    schema_version: Literal[16, 17]
    implementation_digest: Sha256

    @model_validator(mode="after")
    def validate_path(self) -> LabSchedulingQueueIdentity:
        if not Path(self.canonical_job_store_path).is_absolute() or any(value < 0 for value in self.database_generation):
            raise ValueError("scheduling queue requires the original physical DB identity")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self)


class LabSchedulingMaintenanceScope(RuntimeContractModel):
    report_root: Path
    artifact_commit_root: Path
    final_artifact_root: Path

    @model_validator(mode="after")
    def validate_private_roots(self) -> LabSchedulingMaintenanceScope:
        roots = (self.report_root, self.artifact_commit_root, self.final_artifact_root)
        if any(not root.is_absolute() or root != root.resolve(strict=True) for root in roots) or len(set(roots)) != 3:
            raise ValueError("migration requires three distinct original canonical roots")
        return self


class _SchedulingCommand(RuntimeContractModel):
    target_scope: Literal["scheduler"] = "scheduler"
    queue_identity: LabSchedulingQueueIdentity
    expected_version: StrictInt = Field(ge=0, le=_MAX)
    accepted_at: AwareUtcDatetime


class PauseSchedulingCommand(_SchedulingCommand):
    command_type: Literal["pause_scheduling"] = "pause_scheduling"


class ResumeSchedulingCommand(_SchedulingCommand):
    command_type: Literal["resume_scheduling"] = "resume_scheduling"


class LabSchedulingCommandEnvelope(RuntimeContractModel):
    schema_version: Literal[2] = 2
    request_id: UUID
    command: Annotated[PauseSchedulingCommand | ResumeSchedulingCommand, Field(discriminator="command_type")]
    content_hash: Sha256 | Literal[""] = ""

    @model_validator(mode="after")
    def validate_content(self) -> LabSchedulingCommandEnvelope:
        expected = canonical_sha256(self.command)
        if self.content_hash and self.content_hash != expected:
            raise ValueError("scheduler content hash differs from complete original command")
        if self.command.queue_identity.schema_version != 17:
            raise ValueError("scheduler command requires explicit schema17 control capability")
        object.__setattr__(self, "content_hash", expected)
        if len(self.model_dump_json().encode()) > 4096:
            raise ValueError("scheduler envelope exceeds 4 KiB")
        return self


class LabSchedulingCommandReceipt(RuntimeContractModel):
    schema_version: Literal[2] = 2
    request_id: UUID
    content_hash: Sha256
    target_scope: Literal["scheduler"] = "scheduler"
    queue_identity: LabSchedulingQueueIdentity
    status: Literal["applied", "rejected"]
    reason: StrictStr = Field(min_length=1, max_length=128)
    desired_version: StrictInt = Field(ge=0, le=_MAX)
    paused: StrictBool
    completed_at: AwareUtcDatetime


class LabSchedulingControlState(RuntimeContractModel):
    queue_identity: LabSchedulingQueueIdentity
    barrier_identity: Sha256
    desired_version: StrictInt = Field(ge=0, le=_MAX)
    desired_paused: StrictBool
    request_id: UUID | None = None
    accepted_at: AwareUtcDatetime
    applied_version: StrictInt = Field(ge=0, le=_MAX)
    applied_paused: StrictBool
    applied_at: AwareUtcDatetime
    scheduler_fence: StrictInt = Field(ge=1, le=_MAX)
    draining_count: StrictInt = Field(ge=0, le=64)
    observed_at: AwareUtcDatetime

    @model_validator(mode="after")
    def validate_state(self) -> LabSchedulingControlState:
        if self.queue_identity.schema_version != 17 or self.applied_version > self.desired_version:
            raise ValueError("scheduler state has an invalid schema or version order")
        if self.applied_version == self.desired_version and self.applied_paused != self.desired_paused:
            raise ValueError("same-version desired/applied states disagree")
        if self.applied_paused and self.applied_version == self.desired_version and self.draining_count:
            raise ValueError("applied pause still has an unresolved drain")
        if max(self.accepted_at, self.applied_at) > self.observed_at:
            raise ValueError("scheduler state contains facts after observation")
        return self

    @property
    def state_hash(self) -> str:
        return canonical_sha256(self)


class LabSchedulingDrain(RuntimeContractModel):
    kind: Literal["worker", "source"]
    token: StrictStr = Field(min_length=1, max_length=64)
    identity_hash: Sha256


class LabSchedulingBarrier(RuntimeContractModel):
    contract: Literal["lab-scheduling-barrier/v1"] = "lab-scheduling-barrier/v1"
    queue_identity: LabSchedulingQueueIdentity
    root_generation: tuple[StrictInt, StrictInt]
    barrier_identity: Sha256
    state: Literal["migration_pending", "transition_pending", "open", "closed"]
    desired_version: StrictInt = Field(ge=0, le=_MAX)
    desired_paused: StrictBool
    applied_version: StrictInt = Field(default=0, ge=0, le=_MAX)
    applied_paused: StrictBool = False
    scheduler_fence: StrictInt = Field(ge=1, le=_MAX)
    pending_command: LabSchedulingCommandEnvelope | None = None
    drains: tuple[LabSchedulingDrain, ...] = Field(max_length=64)
    source_permits: tuple[LabSchedulingDrain, ...] = Field(default=(), max_length=32)
    observed_at: AwareUtcDatetime
    material_hash: Sha256 | Literal[""] = ""

    @model_validator(mode="after")
    def validate_material(self) -> LabSchedulingBarrier:
        if self.state == "transition_pending" and self.pending_command is None or self.state != "transition_pending" and self.pending_command is not None:
            raise ValueError("scheduler pending marker requires its complete original command")
        if self.state == "open" and self.desired_paused or self.state == "closed" and not self.desired_paused:
            raise ValueError("scheduler barrier state differs from desired pause")
        if self.applied_version > self.desired_version:
            raise ValueError("scheduler barrier applied version exceeds desired version")
        tokens = tuple((item.kind, item.token) for item in self.drains)
        permits = tuple(item.token for item in self.source_permits)
        if len(set(tokens)) != len(tokens) or len(set(permits)) != len(permits):
            raise ValueError("scheduler barrier contains duplicate drain tokens")
        expected = canonical_sha256(self.model_dump(exclude={"material_hash"}))
        if self.material_hash and self.material_hash != expected:
            raise ValueError("scheduler barrier material differs")
        object.__setattr__(self, "material_hash", expected)
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > 8192:
            raise ValueError("scheduler barrier exceeds 8 KiB")
        return self


def validate_scheduling_schema(connection: sqlite3.Connection) -> None:
    from rquant.lab_jobs import _validate_v5_table_sql

    _validate_v5_table_sql(connection, table="lab_scheduler_control", expected=_STATE_SQL)
    _validate_v5_table_sql(connection, table="lab_scheduler_control_receipt", expected=_RECEIPT_SQL)
    state = read_control_state(connection)
    if state is None:
        raise ValueError("schema17 has no original scheduling control state")


def read_control_state(connection: sqlite3.Connection) -> LabSchedulingControlState | None:
    if connection.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='lab_scheduler_control'").fetchone() is None:
        return None
    rows = connection.execute("SELECT singleton,length(CAST(payload_json AS BLOB)) FROM lab_scheduler_control").fetchall()
    if len(rows) != 1 or rows[0][0] != 1 or rows[0][1] > 8192:
        raise ValueError("scheduling control singleton is missing or exceeds budget")
    payload = connection.execute("SELECT payload_json FROM lab_scheduler_control WHERE singleton=1 AND length(CAST(payload_json AS BLOB))<=8192").fetchone()[0]
    return LabSchedulingControlState.model_validate_json(payload)


def _receipt(connection: sqlite3.Connection, request_id: UUID) -> LabSchedulingCommandReceipt | None:
    row = connection.execute("SELECT content_hash,length(CAST(payload_json AS BLOB)) FROM lab_scheduler_control_receipt WHERE request_id=?", (str(request_id),)).fetchone()
    if row is None:
        return None
    if row[1] > 8192:
        raise ValueError("scheduling receipt exceeds budget")
    payload = connection.execute("SELECT payload_json FROM lab_scheduler_control_receipt WHERE request_id=? AND length(CAST(payload_json AS BLOB))<=8192", (str(request_id),)).fetchone()[0]
    receipt = LabSchedulingCommandReceipt.model_validate_json(payload)
    if receipt.request_id != request_id or receipt.content_hash != row[0]:
        raise ValueError("scheduling receipt original identity differs")
    return receipt


def _save_state(connection: sqlite3.Connection, state: LabSchedulingControlState) -> None:
    connection.execute("UPDATE lab_scheduler_control SET payload_json=? WHERE singleton=1", (state.model_dump_json(),))


def _queue_identity(store: LabJobStore, connection: sqlite3.Connection) -> LabSchedulingQueueIdentity:
    return LabSchedulingQueueIdentity.model_validate(store._scheduler_fence_authority(connection))


def scheduling_state(store: LabJobStore) -> LabSchedulingControlState | None:
    with store._read_transaction() as connection:
        state = read_control_state(connection)
        if state is not None and state.queue_identity != _queue_identity(store, connection):
            raise ValueError("scheduling state original queue identity differs")
        return state


def scheduling_reader_state(reader: LabJobReader) -> LabSchedulingControlState | None:
    with reader._read_snapshot(label="scheduler control") as connection:
        state = read_control_state(connection)
        if state is None:
            return None
        identity = state.queue_identity
        observed = reader.path.lstat()
        if str(reader.path.resolve(strict=True)) != identity.canonical_job_store_path or (observed.st_dev, observed.st_ino) != identity.database_generation or connection.execute("PRAGMA application_id").fetchone()[0] != identity.application_id or connection.execute("PRAGMA user_version").fetchone()[0] != identity.schema_version:
            raise ValueError("scheduler read original physical queue identity differs")
        return state


class LabSchedulingSubmission(RuntimeContractModel):
    request_id: UUID
    content_hash: Sha256
    target_scope: Literal["scheduler"] = "scheduler"
    queue_identity: LabSchedulingQueueIdentity
    expected_version: StrictInt = Field(ge=0, le=_MAX)
    status: Literal["pending", "applied", "rejected"]
    spool_path: Path
    spool_generation: tuple[StrictInt, StrictInt]
    receipt: LabSchedulingCommandReceipt | None = None

    @model_validator(mode="after")
    def validate_receipt(self) -> LabSchedulingSubmission:
        if (self.status == "pending") != (self.receipt is None):
            raise ValueError("scheduler submission state lacks its original receipt")
        if self.receipt is not None and (self.receipt.request_id != self.request_id or self.receipt.content_hash != self.content_hash or self.receipt.queue_identity != self.queue_identity or self.receipt.status != self.status):
            raise ValueError("scheduler submission receipt identity differs")
        return self


def scheduling_identity(store: LabJobStore) -> LabSchedulingQueueIdentity:
    with store._read_transaction() as connection:
        return _queue_identity(store, connection)


def scheduling_receipt(store: LabJobStore, request_id: UUID) -> LabSchedulingCommandReceipt | None:
    with store._read_transaction() as connection:
        if read_control_state(connection) is None:
            return None
        return _receipt(connection, request_id)


def scheduling_allows_dispatch(connection: sqlite3.Connection, *, capability_loaded: bool) -> bool:
    state = read_control_state(connection)
    if state is None:
        return True
    return capability_loaded and not state.desired_paused and state.applied_version == state.desired_version and not state.applied_paused


def _assert_quiescent(connection: sqlite3.Connection) -> None:
    checks = (
        "SELECT 1 FROM lab_shard WHERE status='running' LIMIT 1",
        "SELECT 1 FROM lab_claim_publication WHERE status NOT IN ('PUBLISHED','ABORTED') LIMIT 1",
        "SELECT 1 FROM lab_worker_report WHERE status='pending' OR applied_at IS NULL LIMIT 1",
        "SELECT 1 FROM lab_job WHERE result_state='ready' LIMIT 1",
        "SELECT 1 FROM lab_claim_publication_finalizer_lease WHERE released_at IS NULL LIMIT 1",
    )
    if any(connection.execute(statement).fetchone() is not None for statement in checks):
        raise ValueError("schema16 migration requires a quiescent original queue and finalizer")


def enable_scheduling_control(store: LabJobStore, *, lease: LabLeaseRecord, barrier_port: LabSchedulingBarrierPort, now: datetime) -> LabSchedulingControlState:
    if type(barrier_port) is not LabSchedulingBarrierPort or barrier_port._store is not store:
        raise TypeError("migration requires this queue's concrete scheduling metadata port")
    with barrier_port.locked(), barrier_port.migration_window(required=store.scheduling_state() is None):
        with store._transaction() as connection:
            store._validate_lease(connection, lease, now=now)
            current = read_control_state(connection)
            if current is not None:
                if current.queue_identity != _queue_identity(store, connection):
                    raise ValueError("scheduling migration queue identity changed")
                marker = barrier_port._read_locked()
                if marker is None or marker.queue_identity != current.queue_identity or marker.barrier_identity != current.barrier_identity or marker.desired_version != current.desired_version or marker.state not in ("open", "closed", "migration_pending"):
                    raise ValueError("original scheduling barrier must be recovered without repair")
                state = current
            else:
                _assert_quiescent(connection)
                barrier_port._assert_physical_quiescent(connection, now=now)
                old_identity = _queue_identity(store, connection)
                if old_identity.schema_version != 16:
                    raise ValueError("explicit scheduling migration requires exact schema16")
                barrier_port._write_locked(LabSchedulingBarrier(queue_identity=old_identity, root_generation=barrier_port._root_pair, barrier_identity=barrier_port.identity, state="migration_pending", desired_version=0, desired_paused=False, scheduler_fence=lease.fencing_token, drains=(), observed_at=now))
                connection.execute(_STATE_SQL)
                connection.execute(_RECEIPT_SQL)
                connection.execute("PRAGMA user_version=17")
                identity = _queue_identity(store, connection)
                state = LabSchedulingControlState(queue_identity=identity, barrier_identity=barrier_port.identity, desired_version=0, desired_paused=False, applied_version=0, applied_paused=False, accepted_at=now, applied_at=now, scheduler_fence=lease.fencing_token, draining_count=0, observed_at=now)
                connection.execute("INSERT INTO lab_scheduler_control VALUES(1,?)", (state.model_dump_json(),))
        if current is None or marker.state == "migration_pending":
            barrier_port._write_locked(barrier_port._for_state(state, now=now, source_permits=()))
        store.scheduling_control_enabled = True
        return state


class LabSchedulingBarrierPort:
    """Only global metadata and the existing kernel spool lock; no claim publisher."""

    def __init__(self, root: Path, *, store: LabJobStore, maintenance_scope: LabSchedulingMaintenanceScope | None = None) -> None:
        from rquant.lab_job_protocol import LabCommandSpool
        from rquant.lab_jobs import LabJobStore

        if type(store) is not LabJobStore:
            raise TypeError("scheduling metadata requires the original concrete job store")
        self.root = Path(os.path.abspath(root))
        self._store = store
        if maintenance_scope is not None and type(maintenance_scope) is not LabSchedulingMaintenanceScope:
            raise TypeError("migration requires a concrete original maintenance scope")
        self.maintenance_scope = maintenance_scope
        self._maintenance_pairs: dict[Path, tuple[int, int]] = {}
        if maintenance_scope is not None:
            for path in (maintenance_scope.report_root, maintenance_scope.artifact_commit_root, maintenance_scope.final_artifact_root):
                physical = path.lstat()
                LabCommandSpool._validate_private_directory_stat(physical, label="migration original root")
                self._maintenance_pairs[path] = (physical.st_dev, physical.st_ino)
        self._thread_lock = RLock()
        self._active_fd: int | None = None
        observed = self.root.lstat()
        LabCommandSpool._validate_private_directory_stat(observed, label="scheduling spool metadata")
        self._root_pair = (observed.st_dev, observed.st_ino)
        parent = self.root.parent.lstat()
        LabCommandSpool._validate_private_directory_stat(parent, label="scheduling spool lock parent")
        self._parent_pair = (parent.st_dev, parent.st_ino)
        lock_digest = hashlib.sha256(os.fsencode(self.root)).hexdigest()[:16]
        self.lock_path = self.root.parent / f".{self.root.name}.{lock_digest}.spool.lock"
        self.marker_path = self.root / _MARKER
        self.identity = canonical_sha256({"contract": "lab-scheduling-metadata-port/v1", "root": str(self.root), "root_generation": self._root_pair, "queue_path": str(store.path.absolute())})

    @staticmethod
    def _names(descriptor: int) -> tuple[str, ...]:
        names: list[str] = []
        with os.scandir(descriptor) as entries:
            for entry in entries:
                names.append(entry.name)
                if len(names) > 4096:
                    raise ValueError("migration physical namespace exceeds finite budget")
        return tuple(sorted(names))

    @contextmanager
    def migration_window(self, *, required: bool) -> Iterator[None]:
        if not required or self.maintenance_scope is None:
            yield
            return
        descriptors: list[int] = []
        try:
            for root in (self.maintenance_scope.report_root, self.maintenance_scope.artifact_commit_root):
                digest = hashlib.sha256(os.fsencode(root)).hexdigest()[:16]
                path = root.parent / f".{root.name}.{digest}.spool.lock"
                descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
                descriptors.append(descriptor)
                physical = os.fstat(descriptor)
                if not stat.S_ISREG(physical.st_mode) or physical.st_uid != os.getuid() or physical.st_nlink != 1 or stat.S_IMODE(physical.st_mode) != 0o600:
                    raise ValueError("migration requires an original private spool lock")
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as exc:
                    raise ValueError("migration requires quiescent original reports and finalizer") from exc
                current = path.lstat()
                if (current.st_dev, current.st_ino) != (physical.st_dev, physical.st_ino):
                    raise ValueError("migration original spool lock changed")
            root = self.maintenance_scope.final_artifact_root / "finalization-locks"
            directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                for name in self._names(directory):
                    descriptor = os.open(name, os.O_RDWR | os.O_NOFOLLOW, dir_fd=directory)
                    descriptors.append(descriptor)
                    physical = os.fstat(descriptor)
                    if not stat.S_ISREG(physical.st_mode) or physical.st_uid != os.getuid() or physical.st_nlink != 1 or stat.S_IMODE(physical.st_mode) != 0o600:
                        raise ValueError("migration original finalizer lock is invalid")
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError as exc:
                        raise ValueError("migration requires quiescent original finalizer") from exc
            finally:
                os.close(directory)
            yield
        finally:
            for descriptor in reversed(descriptors):
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    def _assert_physical_quiescent(self, connection: sqlite3.Connection, *, now: datetime) -> None:
        from rquant.lab_shard_protocol import LabExecutionAdmission, LabReportReceipt, LabWorkerReport

        if self._active_fd is None:
            raise ValueError("migration requires the original locked claim namespace")
        for relative in ("pending", "current", "admitted/.tmp"):
            descriptor = os.open(relative, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._active_fd)
            try:
                if self._names(descriptor):
                    raise ValueError("migration requires quiescent original claim delivery and admission")
            finally:
                os.close(descriptor)
        descriptor = os.open("admitted", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._active_fd)
        try:
            for name in self._names(descriptor):
                if name == ".tmp":
                    continue
                token = UUID(name.removesuffix(".json"))
                if name != f"{token}.json":
                    raise ValueError("migration admission name differs from its original token")
                file = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=descriptor)
                try:
                    physical = os.fstat(file)
                    if not stat.S_ISREG(physical.st_mode) or physical.st_nlink != 1 or physical.st_size > 1024 * 1024:
                        raise ValueError("migration original admission exceeds its bounded regular file")
                    admission = strict_model_validate_canonical_json(LabExecutionAdmission, os.read(file, 1024 * 1024 + 1))
                finally:
                    os.close(file)
                claim = admission.claim
                if claim.claim_token != token:
                    raise ValueError("migration admission original token differs")
                row = connection.execute("SELECT CASE WHEN length(CAST(report_json AS BLOB))<=1048576 THEN report_json END,CASE WHEN length(CAST(receipt_json AS BLOB))<=8192 THEN receipt_json END FROM lab_worker_report WHERE job_id=? AND shard_id=? AND claim_generation=? AND scheduler_fencing_token=? AND status='accepted' AND report_type='shard_succeeded' ORDER BY applied_at DESC LIMIT 1", (str(claim.job_id), str(claim.shard_id), claim.claim_generation, claim.scheduler_fencing_token)).fetchone()
                if row is None or any(value is None for value in row):
                    raise ValueError("migration admission has no actual accepted terminal cleanup report")
                report, receipt = LabWorkerReport.model_validate_json(row[0]), LabReportReceipt.model_validate_json(row[1])
                if report.claim_token != token or report.payload_hash != claim.payload_hash or report.spec_hash != claim.spec_hash or report.worker_id != claim.worker_id or report.reported_at > now or receipt.content_hash != report.content_hash or receipt.report_id != report.report_id or receipt.claim_token != token or receipt.status != "accepted":
                    raise ValueError("migration original terminal admission/report identity differs")
        finally:
            os.close(descriptor)
        if self.maintenance_scope is None:
            if connection.execute("SELECT 1 FROM lab_shard LIMIT 1").fetchone() is not None or connection.execute("SELECT 1 FROM lab_artifact_commit LIMIT 1").fetchone() is not None:
                raise ValueError("migration with prior effects requires original report and finalizer scope")
            return
        for root, relatives in (
            (self.maintenance_scope.report_root, ("pending",)),
            (self.maintenance_scope.artifact_commit_root, ("pending",)),
            (self.maintenance_scope.final_artifact_root, ("candidates", "seal-intents", "namespace-guard-active")),
        ):
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                physical = os.fstat(descriptor)
                if (physical.st_dev, physical.st_ino) != self._maintenance_pairs[root]:
                    raise ValueError("migration original maintenance root changed")
                for relative in relatives:
                    child = os.open(relative, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
                    try:
                        if self._names(child):
                            raise ValueError("migration requires no pending report or finalizer intent")
                    finally:
                        os.close(child)
            finally:
                os.close(descriptor)

    @contextmanager
    def locked(self) -> Iterator[None]:
        from rquant.lab_job_protocol import LabCommandSpool

        with self._thread_lock:
            if self._active_fd is not None:
                raise ValueError("scheduling metadata does not recursively acquire the claim lock")
            parent_fd = os.open(self.root.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            lock_fd = -1
            root_fd = -1
            try:
                parent = os.fstat(parent_fd)
                if (parent.st_dev, parent.st_ino) != self._parent_pair:
                    raise ValueError("scheduling spool lock parent identity changed")
                lock_fd = os.open(self.lock_path.name, os.O_RDWR | os.O_NOFOLLOW, dir_fd=parent_fd)
                observed = os.fstat(lock_fd)
                if not stat.S_ISREG(observed.st_mode) or observed.st_uid != os.getuid() or observed.st_nlink != 1 or stat.S_IMODE(observed.st_mode) != 0o600:
                    raise ValueError("scheduling requires the original owned 0600 spool lock")
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                active = os.stat(self.lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
                if (active.st_dev, active.st_ino) != (observed.st_dev, observed.st_ino):
                    raise ValueError("original scheduling claim lock was replaced")
                root_fd = os.open(self.root.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
                root = os.fstat(root_fd)
                LabCommandSpool._validate_private_directory_stat(root, label="scheduling spool metadata")
                if (root.st_dev, root.st_ino) != self._root_pair:
                    raise ValueError("scheduling metadata root identity changed")
                self._active_fd = root_fd
                yield
                again = os.stat(self.root.name, dir_fd=parent_fd, follow_symlinks=False)
                if (again.st_dev, again.st_ino) != self._root_pair:
                    raise ValueError("scheduling metadata root changed during transition")
            finally:
                self._active_fd = None
                if root_fd >= 0:
                    os.close(root_fd)
                if lock_fd >= 0:
                    fcntl.flock(lock_fd, fcntl.LOCK_UN)
                    os.close(lock_fd)
                os.close(parent_fd)

    def _read_locked(self) -> LabSchedulingBarrier | None:
        if self._active_fd is None:
            raise ValueError("scheduling metadata is outside the original claim lock")
        return read_scheduling_barrier_at(self._active_fd, expected_root=self._root_pair, expected_identity=self.identity)

    def read_barrier(self) -> LabSchedulingBarrier | None:
        with self.locked():
            return self._read_locked()

    def _write_locked(self, value: LabSchedulingBarrier) -> None:
        if self._active_fd is None or value.root_generation != self._root_pair or value.barrier_identity != self.identity:
            raise ValueError("scheduling marker has no exact locked metadata authority")
        payload = canonical_model_json_bytes(LabSchedulingBarrier.model_validate(value))
        name = f".scheduling-{uuid4().hex}.tmp"
        fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=self._active_fd)
        try:
            with os.fdopen(fd, "wb", closefd=True) as target:
                target.write(payload)
                target.flush()
                os.fsync(target.fileno())
            os.replace(name, _MARKER, src_dir_fd=self._active_fd, dst_dir_fd=self._active_fd)
            os.fsync(self._active_fd)
        finally:
            try:
                os.unlink(name, dir_fd=self._active_fd)
            except FileNotFoundError:
                pass
        if self._read_locked() != value:
            raise ValueError("scheduling marker readback differs")

    def _for_state(self, state: LabSchedulingControlState, *, now: datetime, source_permits: tuple[LabSchedulingDrain, ...], drains: tuple[LabSchedulingDrain, ...] = ()) -> LabSchedulingBarrier:
        return LabSchedulingBarrier(queue_identity=state.queue_identity, root_generation=self._root_pair, barrier_identity=self.identity, state="closed" if state.desired_paused else "open", desired_version=state.desired_version, desired_paused=state.desired_paused, applied_version=state.applied_version, applied_paused=state.applied_paused, scheduler_fence=state.scheduler_fence, drains=drains, source_permits=source_permits, observed_at=now)

    def _worker_drains(self, connection: sqlite3.Connection) -> tuple[LabSchedulingDrain, ...]:
        from rquant.lab_shard_protocol import LabExecutionAdmission

        if self._active_fd is None:
            raise ValueError("worker drain snapshot requires the original claim lock")
        rows = connection.execute("SELECT claim_token FROM lab_shard WHERE status='running' AND claim_token IS NOT NULL LIMIT 65").fetchall()
        if len(rows) > 64:
            raise ValueError("current scheduling drain exceeds budget")
        admitted_fd = os.open("admitted", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._active_fd)
        try:
            result: list[LabSchedulingDrain] = []
            for row in rows:
                token = str(UUID(row[0]))
                try:
                    fd = os.open(f"{token}.json", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=admitted_fd)
                except FileNotFoundError:
                    continue
                try:
                    payload = os.read(fd, 1024 * 1024 + 1)
                    if len(payload) > 1024 * 1024:
                        raise ValueError("original admitted execution exceeds budget")
                    admission = strict_model_validate_canonical_json(LabExecutionAdmission, payload.decode())
                    if str(admission.claim.claim_token) != token:
                        raise ValueError("original execution drain token differs")
                    execution = read_scheduling_execution_at(self._active_fd, UUID(token))
                    if execution is not None:
                        execution.require_claim(admission.claim, marker=self._read_locked())
                        if execution.closed_at is not None:
                            continue
                    result.append(LabSchedulingDrain(kind="worker", token=token, identity_hash=canonical_sha256(admission.claim)))
                finally:
                    os.close(fd)
            return tuple(result)
        finally:
            os.close(admitted_fd)

    def apply_command(self, envelope: LabSchedulingCommandEnvelope, *, lease: LabLeaseRecord, now: datetime) -> LabSchedulingCommandReceipt:
        envelope = LabSchedulingCommandEnvelope.model_validate(envelope)
        with self.locked():
            with self._store._transaction() as connection:
                self._store._validate_lease(connection, lease, now=now)
                state = read_control_state(connection)
                if state is None or state.queue_identity != _queue_identity(self._store, connection):
                    raise ValueError("global scheduling capability is absent or queue changed")
                previous = _receipt(connection, envelope.request_id)
                if previous is not None:
                    if previous.content_hash != envelope.content_hash:
                        raise ValueError("scheduler original request content conflict")
                    return previous
                if state.queue_identity != envelope.command.queue_identity:
                    raise ValueError("scheduler command original queue identity differs")
                if connection.execute("SELECT COUNT(*) FROM lab_scheduler_control_receipt").fetchone()[0] >= 4096:
                    raise ValueError("global scheduling history exceeds 4096 commands")
                marker = self._read_locked()
                if marker is None or marker.queue_identity != state.queue_identity or marker.desired_version != state.desired_version or marker.state not in ("open", "closed"):
                    raise ValueError("global scheduling barrier requires original transition recovery")
                paused = isinstance(envelope.command, PauseSchedulingCommand)
                accepted = envelope.command.expected_version == state.desired_version and state.desired_version < _MAX
                receipt = LabSchedulingCommandReceipt(request_id=envelope.request_id, content_hash=envelope.content_hash, queue_identity=state.queue_identity, status="applied" if accepted else "rejected", reason="desired_accepted" if accepted else "stale_version", desired_version=state.desired_version + int(accepted), paused=paused if accepted else state.desired_paused, completed_at=now)
                if accepted:
                    drains = self._worker_drains(connection) + marker.source_permits if paused else ()
                    pending = LabSchedulingBarrier.model_validate(marker.model_dump() | {"state": "transition_pending", "pending_command": envelope, "scheduler_fence": lease.fencing_token, "drains": drains, "observed_at": now, "material_hash": ""})
                    self._write_locked(pending)
                    state = LabSchedulingControlState.model_validate(state.model_dump() | {"desired_version": receipt.desired_version, "desired_paused": paused, "request_id": envelope.request_id, "accepted_at": now, "scheduler_fence": lease.fencing_token, "draining_count": len(drains), "observed_at": now})
                    _save_state(connection, state)
                connection.execute("INSERT INTO lab_scheduler_control_receipt VALUES(?,?,?)", (str(envelope.request_id), envelope.content_hash, receipt.model_dump_json()))
            if accepted:
                self._write_locked(self._for_state(state, now=now, source_permits=marker.source_permits, drains=drains))
            return receipt

    def source_emit_permission(self, record: LabClaimPublicationRecord, *, lease: LabLeaseRecord, now: datetime) -> bool:
        from rquant.lab_claim_publication import LabClaimPublicationRecord
        from rquant.lab_jobs import _claim_publication_record_from_row

        record = LabClaimPublicationRecord.model_validate(record)
        permit = LabSchedulingDrain(kind="source", token=str(record.identity.attempt_id), identity_hash=canonical_sha256(record.identity))
        with self.locked():
            with self._store._read_transaction() as connection:
                self._store._validate_lease(connection, lease, now=now)
                state = read_control_state(connection)
                marker = self._read_locked()
                if state is None or marker is None or state.queue_identity != _queue_identity(self._store, connection) or marker.queue_identity != state.queue_identity or marker.barrier_identity != state.barrier_identity:
                    raise ValueError("source emit has no original scheduling authority")
                row = connection.execute("SELECT * FROM lab_claim_publication WHERE attempt_id=?", (permit.token,)).fetchone()
                if row is None or _claim_publication_record_from_row(row).identity != record.identity:
                    raise ValueError("source emit original publication identity differs")
                existing = next((item for item in marker.source_permits if item.token == permit.token), None)
                if existing is not None:
                    if existing != permit:
                        raise ValueError("source emit original permit identity differs")
                    return marker.state in ("open", "closed") and (marker.state == "open" or existing in marker.drains)
                if marker.state != "open" or marker.desired_version != state.desired_version or marker.scheduler_fence != lease.fencing_token or not scheduling_allows_dispatch(connection, capability_loaded=True):
                    return False
                if len(marker.source_permits) >= 32:
                    raise ValueError("source emit permit budget exceeds 32")
                updated = LabSchedulingBarrier.model_validate(marker.model_dump() | {"source_permits": marker.source_permits + (permit,), "observed_at": now, "material_hash": ""})
            self._write_locked(updated)
            return True

    def reconcile(self, *, lease: LabLeaseRecord, now: datetime) -> LabSchedulingControlState:
        with self.locked():
            marker = self._read_locked()
            with self._store._transaction() as connection:
                self._store._validate_lease(connection, lease, now=now)
                state = read_control_state(connection)
                if state is None or marker is None or state.queue_identity != _queue_identity(self._store, connection) or marker.queue_identity != state.queue_identity or marker.barrier_identity != state.barrier_identity:
                    raise ValueError("scheduling recovery original identity differs")
                if marker.state == "transition_pending":
                    request = marker.pending_command
                    assert request is not None
                    receipt = _receipt(connection, request.request_id)
                    if receipt is not None:
                        if receipt.content_hash != request.content_hash or receipt.desired_version != state.desired_version:
                            raise ValueError("pending scheduler transition receipt conflicts")
                        marker = self._for_state(state, now=now, source_permits=marker.source_permits, drains=marker.drains)
                    elif state.desired_version == request.command.expected_version:
                        marker = self._for_state(state, now=now, source_permits=marker.source_permits)
                    else:
                        raise ValueError("pending scheduler transition has no exact acceptance proof")
                if marker.desired_version != state.desired_version or marker.desired_paused != state.desired_paused:
                    raise ValueError("scheduling barrier and original desired state differ")
                source_permits: list[LabSchedulingDrain] = []
                for permit in marker.source_permits:
                    from rquant.lab_jobs import _claim_publication_record_from_row

                    row = connection.execute("SELECT * FROM lab_claim_publication WHERE attempt_id=?", (permit.token,)).fetchone()
                    if row is None:
                        source_permits.append(permit)
                        continue
                    publication = _claim_publication_record_from_row(row)
                    if canonical_sha256(publication.identity) != permit.identity_hash:
                        raise ValueError("source drain original publication identity differs")
                    if publication.status.value not in ("PUBLISHED", "ABORTED"):
                        source_permits.append(permit)
                remaining: list[LabSchedulingDrain] = []
                for drain in marker.drains:
                    if drain.kind == "source":
                        if drain in source_permits:
                            remaining.append(drain)
                        continue
                    execution = read_scheduling_execution_at(self._active_fd, UUID(drain.token))
                    if execution is None or execution.claim_hash != drain.identity_hash or execution.closed_at is None:
                        remaining.append(drain)
                change: dict[str, object] = {"scheduler_fence": lease.fencing_token, "draining_count": len(remaining), "observed_at": now}
                if not remaining:
                    change |= {"applied_version": state.desired_version, "applied_paused": state.desired_paused, "applied_at": now}
                state = LabSchedulingControlState.model_validate(state.model_dump() | change)
                _save_state(connection, state)
                marker = self._for_state(state, now=now, source_permits=tuple(source_permits), drains=tuple(remaining))
            self._write_locked(marker)
            self._store.scheduling_control_enabled = True
            return state


def read_scheduling_barrier_at(root_fd: int, *, expected_root: tuple[int, int], expected_identity: str | None = None) -> LabSchedulingBarrier | None:
    try:
        fd = os.open(_MARKER, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root_fd)
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > 8192 or stat.S_IMODE(before.st_mode) != 0o600 or before.st_uid != os.getuid():
            raise ValueError("scheduling marker is not a bounded private original file")
        payload = os.read(fd, 8193)
        after = os.fstat(fd)
        if len(payload) > 8192 or (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size) != (after.st_dev, after.st_ino, after.st_mtime_ns, after.st_size):
            raise ValueError("scheduling marker changed during read")
        marker = strict_model_validate_canonical_json(LabSchedulingBarrier, payload.decode())
        if marker.root_generation != expected_root or expected_identity is not None and marker.barrier_identity != expected_identity:
            raise ValueError("scheduling marker original physical identity differs")
        return marker
    finally:
        os.close(fd)


class LabSchedulingExecution(RuntimeContractModel):
    contract: Literal["lab-scheduling-execution/v1"] = "lab-scheduling-execution/v1"
    claim_token: UUID
    claim_hash: Sha256
    barrier_identity: Sha256
    root_generation: tuple[StrictInt, StrictInt]
    queue_fingerprint: Sha256
    intent_at: AwareUtcDatetime | None = None
    closed_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def validate_times(self) -> LabSchedulingExecution:
        if self.intent_at is None and self.closed_at is None:
            raise ValueError("scheduling execution has neither original intent nor cleanup proof")
        if self.intent_at is not None and self.closed_at is not None and self.closed_at < self.intent_at:
            raise ValueError("scheduling execution cleanup precedes ACK intent")
        return self

    def require_claim(self, claim: LabSpoolClaim, *, marker: LabSchedulingBarrier | None) -> None:
        if marker is None or self.claim_token != claim.claim_token or self.claim_hash != canonical_sha256(claim) or self.barrier_identity != marker.barrier_identity or self.root_generation != marker.root_generation or self.queue_fingerprint != marker.queue_identity.fingerprint:
            raise ValueError("scheduling execution original claim or queue identity differs")


def _execution_name(token: UUID) -> str:
    return f".scheduling-execution-{token}.json"


def read_scheduling_execution_at(root_fd: int, token: UUID) -> LabSchedulingExecution | None:
    try:
        fd = os.open(_execution_name(token), os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root_fd)
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > 4096 or stat.S_IMODE(before.st_mode) != 0o600 or before.st_uid != os.getuid():
            raise ValueError("scheduling ACK intent is not a bounded private file")
        payload = os.read(fd, 4097)
        after = os.fstat(fd)
        current = os.stat(_execution_name(token), dir_fd=root_fd, follow_symlinks=False)
        if len(payload) > 4096 or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) or (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
            raise ValueError("scheduling ACK intent changed during read")
        result = strict_model_validate_canonical_json(LabSchedulingExecution, payload.decode())
        if result.claim_token != token:
            raise ValueError("scheduling ACK intent token differs from original basename")
        return result
    finally:
        os.close(fd)


def write_scheduling_execution_at(root_fd: int, value: LabSchedulingExecution) -> None:
    payload = canonical_model_json_bytes(LabSchedulingExecution.model_validate(value))
    if len(payload) > 4096:
        raise ValueError("scheduling ACK intent exceeds 4 KiB")
    if read_scheduling_execution_at(root_fd, value.claim_token) is None:
        with os.scandir(root_fd) as entries:
            count = sum(entry.name.startswith(".scheduling-execution-") and entry.name.endswith(".json") for entry in entries)
        if count >= 4096:
            raise ValueError("scheduling ACK history exceeds 4096 records")
    temporary = f".scheduling-ack-{uuid4().hex}.tmp"
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=root_fd)
    try:
        with os.fdopen(fd, "wb") as target:
            target.write(payload)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, _execution_name(value.claim_token), src_dir_fd=root_fd, dst_dir_fd=root_fd)
        os.fsync(root_fd)
    finally:
        try:
            os.unlink(temporary, dir_fd=root_fd)
        except FileNotFoundError:
            pass
    if read_scheduling_execution_at(root_fd, value.claim_token) != value:
        raise ValueError("scheduling ACK intent readback differs")


def require_scheduling_claim(marker: LabSchedulingBarrier | None, claim: LabSpoolClaim, *, expected_identity: str | None, admitted: bool, for_ack: bool = False) -> None:
    if marker is None:
        if expected_identity is not None:
            raise ValueError("scheduling capability has no original barrier")
        return
    if expected_identity is None or marker.barrier_identity != expected_identity:
        raise ValueError("scheduling barrier requires its exact installed capability")
    if marker.state not in ("open", "closed"):
        raise ValueError("scheduling transition blocks execution admission and ACK")
    if marker.state == "closed":
        if for_ack:
            raise ValueError("paused scheduling blocks a first ACK")
        if not admitted or not any(item.kind == "worker" and item.token == str(claim.claim_token) and item.identity_hash == canonical_sha256(claim) for item in marker.drains):
            raise ValueError("paused scheduling has no original admitted drain for this claim")
        return
    if marker.applied_version != marker.desired_version or marker.applied_paused:
        raise ValueError("scheduling resume is not yet applied")
    if claim.scheduler_fencing_token != marker.scheduler_fence:
        raise ValueError("scheduling execution has a superseded scheduler fence")
