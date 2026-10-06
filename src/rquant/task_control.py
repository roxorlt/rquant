"""Task effects and confirmations inside the original PageControl journal."""

from __future__ import annotations

import os
import sqlite3
import stat
import time
from collections.abc import Callable
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Literal
from uuid import uuid4

from pydantic import Field, StrictStr, model_validator

from rquant.backtest.contracts import Sha256
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.task_control_commands import (
    OwnedPrepareUnitRun, OwnedRequestUnitRun, OwnedSetLabSchedulingPaused, OwnedTaskControl,
    PrepareUnitRun, RequestUnitRun, SetLabSchedulingPaused, TaskControlRequest,
    TaskControlIdentity, TaskUnitAcceptedContext, TaskSchedulingAcceptedContext, TaskUnitRunDraft,
)
from rquant.task_unit_control import BootId, Counter, JobPath, TaskUnitRunEvidence, TaskSystemdRunWindow, bind_systemd_unit_run, validate_task_window_extension

if TYPE_CHECKING:
    from rquant.page_control import PageControlOutbox
    from rquant.task_control_admission import TaskCenterServingSource
    from rquant.task_unit_control import SystemdUnitRunExecutor
    from rquant.lab_job_center import LabCommandSubmissionFacade


class TaskUnitConfirmation(RuntimeContractModel):
    confirmation_id: StrictStr = Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
    prepare_id: StrictStr
    owner_id: StrictStr
    run: TaskUnitRunDraft
    draft_hash: Sha256
    context: TaskUnitAcceptedContext
    accepted_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    consumed_by: StrictStr | None = None

    @model_validator(mode="after")
    def validate_frozen_draft(self) -> TaskUnitConfirmation:
        if self.draft_hash != self.run.draft_hash or self.expires_at != self.accepted_at + timedelta(minutes=5):
            raise ValueError("unit confirmation differs from its complete frozen draft or expiry")
        if self.consumed_by is not None and self.consumed_by != self.run.command_id:
            raise ValueError("unit confirmation was consumed by a different original UUID")
        return self


class TaskUnitEffect(RuntimeContractModel):
    command: OwnedRequestUnitRun
    stage: Literal["prepared", "start_intent", "acknowledged", "started", "completed", "unknown", "rejected"]
    prepared_at: AwareUtcDatetime
    updated_at: AwareUtcDatetime
    intent_at: AwareUtcDatetime | None = None
    intent_monotonic_ns: Counter | None = None
    job_path: JobPath | None = None
    run: TaskUnitRunEvidence | None = None
    window: TaskSystemdRunWindow | None = None
    reason: StrictStr | None = Field(default=None, max_length=256)
    fenced_by_boot: BootId | None = None

    @model_validator(mode="after")
    def validate_stage(self) -> TaskUnitEffect:
        if self.prepared_at < self.command.accepted_at or self.updated_at < self.prepared_at:
            raise ValueError("unit effect server time rolled back")
        attempted = self.stage not in ("prepared", "rejected")
        if attempted != (self.intent_at is not None and self.intent_monotonic_ns is not None):
            raise ValueError("unit attempt requires its durable start intent")
        if self.intent_at is not None and not self.prepared_at <= self.intent_at <= self.updated_at:
            raise ValueError("unit intent is outside persisted effect times")
        if self.stage in ("acknowledged", "started", "completed") and self.job_path is None:
            raise ValueError("unit acknowledged stage requires its original job")
        if self.stage in ("started", "completed") and self.run is None:
            raise ValueError("unit run stage requires its same-invocation facts")
        if (self.run is None) != (self.window is None) or self.window is not None and bind_systemd_unit_run(self.window) != self.run:
            raise ValueError("unit effect requires its complete original producer facts")
        if self.run is not None:
            command, run = self.command, self.run
            if (
                (run.host_name, run.boot_id, run.unit, run.manifest_digest, run.request_id, run.request_hash)
                != (command.context.host_name, command.context.boot_id, command.unit, command.context.manifest_digest, command.command_id, command.original_request_hash)
                or run.job_witness is None or run.job_witness.job_path != self.job_path
                or run.observed_at > self.updated_at
                or self.intent_at is None or run.started_at < self.intent_at
                or self.intent_monotonic_ns is None or run.job_witness.call_monotonic_ns < self.intent_monotonic_ns
                or self.stage != "unknown" and (self.stage == "completed") != (run.ended_at is not None)
                or self.window.previous_invocation_id != command.context.runtime_state.invocation_id
            ):
                raise ValueError("unit result differs from original accepted effect identity")
        if self.fenced_by_boot is not None and (self.stage != "unknown" or self.fenced_by_boot == self.command.context.boot_id):
            raise ValueError("old unit intent requires a genuinely different observed boot fence")
        if len(self.model_dump_json().encode()) > 32 * 1024:
            raise ValueError("unit effect exceeds 32 KiB")
        return self


def validate_task_journal_command(connection: sqlite3.Connection, command: OwnedTaskControl, identity: TaskControlIdentity) -> None:
    from rquant.page_control import _command_hash

    if command.metadata_identity != identity:
        raise ValueError("task command original metadata identity differs")
    row = connection.execute("SELECT command_kind,command_hash,CASE WHEN length(CAST(payload_json AS BLOB))<=32768 THEN payload_json END,length(CAST(payload_json AS BLOB)) FROM page_control_command WHERE command_id=?", (command.command_id,)).fetchone()
    if row is None or row[3] > 32 * 1024 or row[0] != command.kind or row[1] != _command_hash(command) or row[2] != command.model_dump_json():
        raise ValueError("task effect has no exact original accepted journal command")


class TaskControlJournal:
    """Finite derived unit indexes; command/effect authority stays in PageControl."""

    def __init__(self, outbox: PageControlOutbox) -> None:
        from rquant.page_control import PageControlOutbox

        if type(outbox) is not PageControlOutbox:
            raise TypeError("task controls require the original concrete PageControl outbox")
        self.outbox = outbox
        self.path = Path(os.path.abspath(outbox.path))
        before = self._physical()
        connection = outbox._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='page_control_task_authority'").fetchone()
            if exists is None:
                connection.execute("CREATE TABLE page_control_task_authority(singleton INTEGER PRIMARY KEY CHECK(singleton=1), instance_id TEXT NOT NULL)")
                connection.execute("CREATE TABLE page_control_unit_confirmation(confirmation_id TEXT PRIMARY KEY, prepare_id TEXT NOT NULL UNIQUE REFERENCES page_control_command(command_id), payload_json TEXT NOT NULL)")
                connection.execute("CREATE TABLE page_control_unit_run(command_id TEXT PRIMARY KEY REFERENCES page_control_command(command_id), payload_json TEXT NOT NULL)")
                connection.execute("CREATE TABLE page_control_unit_index(unit TEXT PRIMARY KEY, command_id TEXT NOT NULL REFERENCES page_control_unit_run(command_id), boot_id TEXT NOT NULL)")
                connection.execute("INSERT INTO page_control_task_authority VALUES(1,?)", (uuid4().hex,))
            else:
                tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
                if not {"page_control_unit_confirmation", "page_control_unit_run", "page_control_unit_index"}.issubset(tables):
                    raise ValueError("original task journal metadata is incomplete")
            row = connection.execute("SELECT instance_id FROM page_control_task_authority WHERE singleton=1").fetchone()
            if row is None:
                raise ValueError("original task journal instance is absent")
            self._identity = TaskControlIdentity(path=str(self.path), device=before.st_dev, inode=before.st_ino, instance_id=row[0])
            if (before.st_dev, before.st_ino) != self._physical_pair():
                raise ValueError("task journal replacement during activation")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _physical(self) -> os.stat_result:
        try:
            value = self.path.lstat()
        except OSError as exc:
            raise ValueError("original task journal identity is unavailable") from exc
        if not stat.S_ISREG(value.st_mode) or value.st_nlink != 1:
            raise ValueError("original task journal requires a singly linked regular file")
        return value

    def _physical_pair(self) -> tuple[int, int]:
        value = self._physical()
        return value.st_dev, value.st_ino

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        if self._physical_pair() != (self._identity.device, self._identity.inode):
            raise ValueError("original task journal identity replacement")
        connection = self.outbox._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT instance_id FROM page_control_task_authority WHERE singleton=1").fetchone()
            if row is None or row[0] != self._identity.instance_id or self._physical_pair() != (self._identity.device, self._identity.inode):
                raise ValueError("original task journal instance identity differs")
            yield connection
            if self._physical_pair() != (self._identity.device, self._identity.inode):
                raise ValueError("original task journal identity changed during effect")
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def identity(self) -> TaskControlIdentity:
        with self._transaction():
            return self._identity

    def _original(self, connection: sqlite3.Connection, command: OwnedTaskControl) -> None:
        validate_task_journal_command(connection, command, self._identity)

    @staticmethod
    def _effect(connection: sqlite3.Connection, command: OwnedRequestUnitRun) -> TaskUnitEffect | None:
        row = connection.execute("SELECT CASE WHEN length(CAST(payload_json AS BLOB))<=32768 THEN payload_json END,length(CAST(payload_json AS BLOB)) FROM page_control_unit_run WHERE command_id=?", (command.command_id,)).fetchone()
        if row is None:
            return None
        if row[1] > 32 * 1024:
            raise ValueError("stored unit effect exceeds byte budget")
        effect = TaskUnitEffect.model_validate_json(row[0])
        if effect.command != command:
            raise ValueError("unit effect differs from original journal request")
        return effect

    @staticmethod
    def _save_effect(connection: sqlite3.Connection, effect: TaskUnitEffect) -> None:
        connection.execute("INSERT INTO page_control_unit_run(command_id,payload_json) VALUES(?,?) ON CONFLICT(command_id) DO UPDATE SET payload_json=excluded.payload_json", (effect.command.command_id, effect.model_dump_json()))

    def prepare_confirmation(self, command: OwnedPrepareUnitRun) -> TaskUnitConfirmation:
        with self._transaction() as connection:
            self._original(connection, command)
            row = connection.execute("SELECT CASE WHEN length(CAST(payload_json AS BLOB))<=4096 THEN payload_json END,length(CAST(payload_json AS BLOB)) FROM page_control_unit_confirmation WHERE prepare_id=?", (command.command_id,)).fetchone()
            if row is not None:
                if row[1] > 4096:
                    raise ValueError("stored unit confirmation exceeds byte budget")
                result = TaskUnitConfirmation.model_validate_json(row[0])
                if result.run != command.run or result.owner_id != command.owner_id or result.context != command.context:
                    raise ValueError("unit confirmation differs from original prepare")
                return result
            result = TaskUnitConfirmation(confirmation_id=str(uuid4()), prepare_id=command.command_id, owner_id=command.owner_id, run=command.run, draft_hash=command.run.draft_hash, context=command.context, accepted_at=command.accepted_at, expires_at=command.accepted_at + timedelta(minutes=5))
            if len(result.model_dump_json().encode()) > 4096:
                raise ValueError("unit confirmation exceeds 4 KiB")
            connection.execute("INSERT INTO page_control_unit_confirmation VALUES(?,?,?)", (result.confirmation_id, command.command_id, result.model_dump_json()))
            return result

    def confirmation(self, confirmation_id: str) -> TaskUnitConfirmation | None:
        with self._transaction() as connection:
            return self._confirmation(connection, confirmation_id)

    @staticmethod
    def _confirmation(connection: sqlite3.Connection, confirmation_id: str) -> TaskUnitConfirmation | None:
        row = connection.execute("SELECT CASE WHEN length(CAST(payload_json AS BLOB))<=4096 THEN payload_json END,length(CAST(payload_json AS BLOB)) FROM page_control_unit_confirmation WHERE confirmation_id=?", (confirmation_id,)).fetchone()
        if row is None:
            return None
        if row[1] > 4096:
            raise ValueError("unit confirmation exceeds byte budget")
        return TaskUnitConfirmation.model_validate_json(row[0])

    def run_effect(self, command: OwnedRequestUnitRun) -> TaskUnitEffect | None:
        with self._transaction() as connection:
            self._original(connection, command)
            return self._effect(connection, command)

    def _indexed_effect(self, connection: sqlite3.Connection, unit: str) -> TaskUnitEffect | None:
        index = connection.execute("SELECT command_id,boot_id FROM page_control_unit_index WHERE unit=?", (unit,)).fetchone()
        if index is None:
            return None
        row = connection.execute("SELECT CASE WHEN length(CAST(payload_json AS BLOB))<=32768 THEN payload_json END,length(CAST(payload_json AS BLOB)) FROM page_control_unit_run WHERE command_id=?", (index[0],)).fetchone()
        if row is None or row[1] > 32 * 1024:
            raise ValueError("unit index has no bounded original effect")
        effect = TaskUnitEffect.model_validate_json(row[0])
        self._original(connection, effect.command)
        if effect.command.command_id != index[0] or effect.command.unit != unit or effect.command.context.boot_id != index[1]:
            raise ValueError("unit index original identity differs")
        return effect

    def assert_unit_available(self, unit: str, boot_id: str) -> None:
        with self._transaction() as connection:
            effect = self._indexed_effect(connection, unit)
            if effect is not None and effect.stage not in ("completed", "rejected") and effect.command.context.boot_id == boot_id:
                raise ValueError("exact unit has an unresolved original request")
            if effect is None and connection.execute("SELECT COUNT(*) FROM page_control_unit_index").fetchone()[0] >= 32:
                raise ValueError("exact unit index exceeds 32 entries")

    def unit_unresolved(self, unit: str, boot_id: str) -> bool:
        with self._transaction() as connection:
            effect = self._indexed_effect(connection, unit)
            return effect is not None and effect.command.context.boot_id == boot_id and effect.stage not in ("completed", "rejected")

    def prepare_run(self, command: OwnedRequestUnitRun, *, now: datetime) -> TaskUnitEffect:
        with self._transaction() as connection:
            self._original(connection, command)
            prior = self._effect(connection, command)
            if prior is not None:
                return prior
            old = self._indexed_effect(connection, command.unit)
            if old is not None:
                if old.stage not in ("completed", "rejected"):
                    if old.command.context.boot_id == command.context.boot_id:
                        raise ValueError("exact unit has an unresolved original request")
                    if old.intent_at is not None:
                        fenced = TaskUnitEffect.model_validate(old.model_dump() | {"stage": "unknown", "updated_at": now, "reason": "boot_changed", "fenced_by_boot": command.context.boot_id})
                        self._save_effect(connection, fenced)
                    elif old.stage == "prepared":
                        self._save_effect(connection, TaskUnitEffect.model_validate(old.model_dump() | {"stage": "rejected", "updated_at": now, "reason": "boot_changed"}))
            if old is None and connection.execute("SELECT COUNT(*) FROM page_control_unit_index").fetchone()[0] >= 32:
                raise ValueError("exact unit index exceeds 32 entries")
            effect = TaskUnitEffect(command=command, stage="prepared", prepared_at=now, updated_at=now)
            self._save_effect(connection, effect)
            connection.execute("INSERT INTO page_control_unit_index(unit,command_id,boot_id) VALUES(?,?,?) ON CONFLICT(unit) DO UPDATE SET command_id=excluded.command_id,boot_id=excluded.boot_id", (command.unit, command.command_id, command.context.boot_id))
            return effect

    def start_intent(self, command: OwnedRequestUnitRun, *, now: datetime, monotonic_ns: int) -> TaskUnitEffect:
        with self._transaction() as connection:
            self._original(connection, command)
            effect = self._effect(connection, command)
            if effect is None:
                raise ValueError("unit start has no original prepared effect")
            if effect.stage != "prepared":
                return effect
            index = connection.execute("SELECT command_id,boot_id FROM page_control_unit_index WHERE unit=?", (command.unit,)).fetchone()
            if index is None or tuple(index) != (command.command_id, command.context.boot_id):
                raise ValueError("unit start no longer owns the original unit index")
            if command.context.mode == "writer":
                challenge = None if command.confirmation_id is None else self._confirmation(connection, command.confirmation_id)
                if challenge is None or challenge.owner_id != command.owner_id or challenge.draft_hash != command.draft_hash or challenge.context != command.context:
                    raise ValueError("writer start requires its exact persisted prepare confirmation")
                if challenge.consumed_by not in (None, command.command_id):
                    raise ValueError("writer confirmation is already consumed")
                if not challenge.accepted_at <= now < challenge.expires_at:
                    raise ValueError("writer confirmation expired or server time rolled back")
                consumed = TaskUnitConfirmation.model_validate(challenge.model_dump() | {"consumed_by": command.command_id})
                connection.execute("UPDATE page_control_unit_confirmation SET payload_json=? WHERE confirmation_id=?", (consumed.model_dump_json(), consumed.confirmation_id))
            intent = TaskUnitEffect.model_validate(effect.model_dump() | {"stage": "start_intent", "intent_at": now, "intent_monotonic_ns": monotonic_ns, "updated_at": now})
            self._save_effect(connection, intent)
            return intent

    def record_run(
        self, command: OwnedRequestUnitRun, *, now: datetime, stage: Literal["acknowledged", "started", "completed", "unknown", "rejected"],
        job_path: str | None = None, run: TaskUnitRunEvidence | None = None, reason: str | None = None,
        window: TaskSystemdRunWindow | None = None,
    ) -> TaskUnitEffect:
        with self._transaction() as connection:
            self._original(connection, command)
            effect = self._effect(connection, command)
            if effect is None:
                raise ValueError("unit result has no original effect")
            if effect.stage in ("completed", "rejected"):
                return effect
            if stage == "rejected" and effect.stage != "prepared" or stage != "rejected" and effect.intent_at is None:
                raise ValueError("unit result cannot bypass its persisted start intent")
            if effect.job_path is not None and job_path is not None and effect.job_path != job_path:
                raise ValueError("unit result cannot change its original job path")
            if effect.run is not None and run is not None and effect.run.invocation_id != run.invocation_id:
                raise ValueError("unit result cannot change its original invocation")
            if effect.window is not None and window is not None:
                validate_task_window_extension(effect.window, window)
            if effect.stage == "started" and stage == "acknowledged" or effect.stage == "unknown" and stage not in ("unknown", "started", "completed"):
                raise ValueError("unit result cannot roll back its original attempt stage")
            updated = TaskUnitEffect.model_validate(effect.model_dump() | {"stage": stage, "updated_at": now, "job_path": job_path or effect.job_path,
                "run": run or effect.run, "window": window or effect.window, "reason": reason})
            self._save_effect(connection, updated)
            return updated


class TaskControlSubmissionReference(RuntimeContractModel):
    """Immutable acceptance identity; it never asserts that a unit succeeded."""

    contract: Literal["task-control-submission/v1"] = "task-control-submission/v1"
    command_id: StrictStr
    kind: Literal["prepare_unit_run", "request_unit_run", "set_lab_scheduling_paused"]
    owner_id: StrictStr
    original_request_hash: Sha256
    metadata_identity: TaskControlIdentity
    accepted_at: AwareUtcDatetime
    accepted_command_hash: Sha256


class TaskControlPageControlBackend:
    def __init__(self, *, journal: TaskControlJournal, source: TaskCenterServingSource, executor: SystemdUnitRunExecutor,
                 operators: tuple[str, ...] = (), scheduling_admins: tuple[str, ...] = (), enabled: bool = False,
                 lab_facade: LabCommandSubmissionFacade | None = None, clock: Callable[[], datetime] | None = None) -> None:
        from rquant.task_control_admission import TaskCenterServingSource
        from rquant.task_unit_control import SystemdUnitRunExecutor
        from rquant.lab_job_center import LabCommandSubmissionFacade

        if type(journal) is not TaskControlJournal or type(source) is not TaskCenterServingSource or type(executor) is not SystemdUnitRunExecutor:
            raise TypeError("task control requires concrete original journal/source/unit leaf")
        if lab_facade is not None and type(lab_facade) is not LabCommandSubmissionFacade:
            raise TypeError("scheduler control requires the original concrete Lab facade")
        if type(enabled) is not bool or any(len(roles) > 16 or len(set(roles)) != len(roles) or any(not isinstance(user, str) or not 1 <= len(user) <= 256 for user in roles) for roles in (operators, scheduling_admins)):
            raise ValueError("task roles require finite separate exact allowlists")
        self.journal, self.source, self.executor = journal, source, executor
        self.operators, self.scheduling_admins, self.enabled = operators, scheduling_admins, enabled
        self.lab_facade, self.clock = lab_facade, clock or (lambda: datetime.now(UTC))

    def authorize(self, actor: str, request: TaskControlRequest | OwnedTaskControl) -> None:
        allowed = self.scheduling_admins if type(request) in (SetLabSchedulingPaused, OwnedSetLabSchedulingPaused) else self.operators
        if actor not in allowed:
            raise PermissionError("task action is unavailable for the current role")

    def compile(self, request: TaskControlRequest, *, authenticated_actor_id: str, expected_identity: TaskControlIdentity) -> OwnedTaskControl:
        from rquant.lab_scheduling_control import LabSchedulingCommandEnvelope, PauseSchedulingCommand, ResumeSchedulingCommand
        from rquant.task_unit_control import guard_task_unit_run

        self.authorize(authenticated_actor_id, request)
        if not self.enabled:
            raise ValueError("task controls are disabled")
        identity = self.journal.identity()
        if identity != expected_identity:
            raise ValueError("task original metadata identity changed before acceptance")
        view = self.source.read(generation_id=request.generation_id)
        now = self.clock()
        common = request.model_dump() | {"owner_id": authenticated_actor_id, "accepted_at": now,
            "metadata_identity": identity, "original_request_hash": request.request_hash}
        if type(request) is SetLabSchedulingPaused:
            if self.lab_facade is None:
                raise ValueError("original scheduler facade is unavailable")
            state = view.scheduling_control
            live = self.lab_facade.reader.scheduling_state()
            if state is None or live is None or state != live or state.desired_version != request.expected_version:
                raise ValueError("original scheduling state or CAS version changed")
            spool = self.lab_facade.spool.root.lstat()
            context = TaskSchedulingAcceptedContext(store_id=state.queue_identity.store_id, queue_binding_fingerprint=state.queue_identity.fingerprint,
                source_payload_hash=view.material_hash, generation_id=view.generation_id, schema_version=17, observed_at=state.observed_at,
                spool_path=str(self.lab_facade.spool.root.resolve(strict=True)), spool_generation=(spool.st_dev, spool.st_ino))
            action = PauseSchedulingCommand if request.paused else ResumeSchedulingCommand
            envelope = LabSchedulingCommandEnvelope(request_id=request.command_id, command=action(queue_identity=state.queue_identity, expected_version=request.expected_version, accepted_at=now))
            return OwnedSetLabSchedulingPaused.model_validate(common | {"context": context, "envelope": envelope})
        if type(request) not in (PrepareUnitRun, RequestUnitRun):
            raise TypeError("task request requires an exact protected kind")
        unit = request.run.unit if type(request) is PrepareUnitRun else request.unit
        manifest, policy = self.executor.configuration()
        runtime = self.executor.read_state(unit)
        mode = guard_task_unit_run(policy, manifest=manifest, state=runtime, unit=unit, now=now).mode
        if (view.snapshot.host_name, view.snapshot.boot_id, view.snapshot.manifest_digest) != (manifest.host_name, runtime.boot_id, manifest.digest):
            raise ValueError("unit source does not match actual host/boot/manifest")
        self.journal.assert_unit_available(unit, runtime.boot_id)
        context = TaskUnitAcceptedContext(host_name=manifest.host_name, boot_id=runtime.boot_id, manifest_digest=manifest.digest,
            policy_digest=policy.digest, source_payload_hash=view.material_hash, generation_id=view.generation_id,
            observed_at=view.snapshot.sampled_at, mode=mode, runtime_state=runtime)
        if type(request) is RequestUnitRun and mode == "writer":
            challenge = None if request.confirmation_id is None else self.journal.confirmation(request.confirmation_id)
            if challenge is None or challenge.owner_id != authenticated_actor_id or challenge.draft_hash != request.draft_hash or challenge.context.model_dump(exclude={"runtime_state"}) != context.model_dump(exclude={"runtime_state"}) or challenge.context.runtime_state.model_dump(exclude={"observed_at"}) != runtime.model_dump(exclude={"observed_at"}):
                raise ValueError("writer request requires its exact original prepare confirmation")
            if challenge.consumed_by not in (None, request.command_id) or not challenge.accepted_at <= now < challenge.expires_at:
                raise ValueError("writer original confirmation is expired or consumed")
            context = challenge.context
        return (OwnedPrepareUnitRun if type(request) is PrepareUnitRun else OwnedRequestUnitRun).model_validate(common | {"context": context})

    def validate(self, command: OwnedTaskControl) -> None:
        self.authorize(command.owner_id, command)
        if self.journal.identity() != command.metadata_identity:
            raise ValueError("task original metadata identity differs")
        if type(command) is OwnedSetLabSchedulingPaused:
            if self.lab_facade is None:
                raise ValueError("original scheduler facade is unavailable")
            root = self.lab_facade.spool.root
            physical = root.lstat()
            if str(root.resolve(strict=True)) != command.context.spool_path or (physical.st_dev, physical.st_ino) != command.context.spool_generation:
                raise ValueError("original scheduler spool identity differs")
            state = self.lab_facade.reader.scheduling_state()
            if state is None or state.queue_identity != command.envelope.command.queue_identity:
                raise ValueError("original scheduler queue identity differs")

    @staticmethod
    def reference(command: OwnedTaskControl) -> dict[str, object]:
        return TaskControlSubmissionReference(command_id=command.command_id, kind=command.kind, owner_id=command.owner_id,
            original_request_hash=command.original_request_hash, metadata_identity=command.metadata_identity,
            accepted_at=command.accepted_at, accepted_command_hash=canonical_sha256(command)).model_dump(mode="json")

    def has_effect(self, command: OwnedTaskControl) -> bool:
        if type(command) is OwnedPrepareUnitRun:
            with self.journal._transaction() as connection:
                self.journal._original(connection, command)
                return connection.execute("SELECT 1 FROM page_control_unit_confirmation WHERE prepare_id=?", (command.command_id,)).fetchone() is not None
        if type(command) is OwnedRequestUnitRun:
            return self.journal.run_effect(command) is not None
        return self.lab_facade is not None and self.lab_facade.spool.find(command.envelope.request_id) is not None

    def submit(self, command: OwnedTaskControl) -> dict[str, object]:
        self.validate(command)
        if type(command) is OwnedPrepareUnitRun:
            self.journal.prepare_confirmation(command)
        elif type(command) is OwnedSetLabSchedulingPaused:
            with self.journal._transaction() as connection:
                self.journal._original(connection, command)
            self.lab_facade.submit_scheduling_control(command.envelope)
        else:
            self._run_once(command)
        return self.reference(command)

    def recover(self, command: OwnedTaskControl) -> dict[str, object] | None:
        self.validate(command)
        if not self.has_effect(command):
            return None
        if type(command) is OwnedRequestUnitRun:
            effect = self.journal.run_effect(command)
            if effect.stage == "prepared":
                self._run_once(command)
            elif effect.stage in ("start_intent", "acknowledged"):
                self.journal.record_run(command, now=self.clock(), stage="unknown", reason="original_start_reply_unavailable")
            elif effect.stage in ("started", "unknown") and effect.window is not None:
                attempt = self.executor.observe(effect.window, policy_digest=command.context.policy_digest)
                self.journal.record_run(command, now=self.clock(), stage=attempt.stage, job_path=attempt.job_path,
                    run=attempt.run, window=attempt.window, reason=attempt.reason)
        elif type(command) is OwnedSetLabSchedulingPaused:
            self.lab_facade.submit_scheduling_control(command.envelope)
        return self.reference(command)

    def _run_once(self, command: OwnedRequestUnitRun) -> None:
        from rquant.page_control import _PageControlExecutionMutex
        from rquant.task_unit_control import guard_task_unit_run

        with _PageControlExecutionMutex(self.journal.path.with_name(self.journal.path.name + ".unit-start.lock")) as acquired:
            if not acquired:
                raise RuntimeError("original unit executor is busy")
            self.validate(command)
            effect = self.journal.prepare_run(command, now=self.clock())
            if effect.stage != "prepared":
                if effect.stage in ("start_intent", "acknowledged"):
                    self.journal.record_run(command, now=self.clock(), stage="unknown", reason="original_start_reply_unavailable")
                return
            try:
                if not self.enabled:
                    raise ValueError("task controls are disabled before original start")
                manifest, policy = self.executor.configuration()
                current = self.executor.read_state(command.unit)
                guard_task_unit_run(policy, manifest=manifest, state=current, unit=command.unit, now=self.clock())
                if (manifest.host_name, current.boot_id, manifest.digest, policy.digest) != (command.context.host_name, command.context.boot_id, command.context.manifest_digest, command.context.policy_digest) or current.model_dump(exclude={"observed_at"}) != command.context.runtime_state.model_dump(exclude={"observed_at"}):
                    raise ValueError("original unit runtime identity changed before start")
                with self.executor.prepare(command.unit, now=self.clock()) as session:
                    intent = self.journal.start_intent(command, now=self.clock(), monotonic_ns=time.monotonic_ns())
                    if intent.stage != "start_intent":
                        return
                    attempt = self.executor.invoke(command_id=command.command_id, request_hash=command.original_request_hash,
                        context_host=command.context.host_name, context_boot=command.context.boot_id,
                        manifest_digest=command.context.manifest_digest, policy_digest=command.context.policy_digest,
                        expected_state=command.context.runtime_state, session=session)
                    self.journal.record_run(command, now=self.clock(), stage=attempt.stage, job_path=attempt.job_path, run=attempt.run, window=attempt.window, reason=attempt.reason)
            except Exception as exc:
                effect = self.journal.run_effect(command)
                self.journal.record_run(command, now=self.clock(), stage="rejected" if effect.intent_at is None else "unknown",
                    reason=f"{type(exc).__name__}: {exc}"[:256])
