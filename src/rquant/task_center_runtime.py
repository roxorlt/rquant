"""Exact task runtime bindings and read-only original unit effect projections."""

from __future__ import annotations

import os
import sqlite3
import stat
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from typing import Literal

from pydantic import Field, StrictBool, StrictInt, model_validator

from rquant.backtest.contracts import CommitSha
from rquant.lab_scheduling_control import LabSchedulingBarrierPort, LabSchedulingMaintenanceScope, LabSchedulingQueueIdentity
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

from rquant.task_control import TaskUnitEffect, validate_task_journal_command
from rquant.task_control_commands import TaskControlIdentity
from rquant.task_unit_control import TaskUnitRunEvidence


class TaskCenterControlProfile(RuntimeContractModel):
    contract: Literal["task-center-control-profile/v1"] = "task-center-control-profile/v1"
    producer_commit: CommitSha
    runtime_root: Path
    enabled: StrictBool = False
    allow_local_migration: StrictBool = False
    queue_identity: LabSchedulingQueueIdentity | None = None
    claim_spool_root: Path | None = None
    claim_spool_generation: tuple[StrictInt, StrictInt] | None = None
    maintenance_scope: LabSchedulingMaintenanceScope | None = None
    maintenance_generations: tuple[tuple[StrictInt, StrictInt], ...] = Field(default=(), max_length=3)
    unit_journal_identity: TaskControlIdentity | None = None

    @model_validator(mode="after")
    def validate_scope(self) -> TaskCenterControlProfile:
        if not self.runtime_root.is_absolute() or self.runtime_root != self.runtime_root.resolve(strict=True):
            raise ValueError("task profile requires its original canonical runtime root")
        fields = (self.queue_identity, self.claim_spool_root, self.claim_spool_generation, self.maintenance_scope)
        if self.enabled and (any(value is None for value in fields) or len(self.maintenance_generations) != 3):
            raise ValueError("enabled task profile requires the complete original queue and maintenance identity")
        if self.allow_local_migration and not self.enabled:
            raise ValueError("local task migration requires an explicit enabled profile")
        if any(value < 0 for pair in self.maintenance_generations + (() if self.claim_spool_generation is None else (self.claim_spool_generation,)) for value in pair):
            raise ValueError("task profile physical generations are invalid")
        if len(self.model_dump_json().encode()) > 16 * 1024:
            raise ValueError("task profile exceeds 16 KiB")
        return self


def load_task_center_control_profile(path: Path, *, producer_commit: str, runtime_root: Path) -> TaskCenterControlProfile:
    from rquant.authority_path_security import read_secure_regular_file
    from rquant.strict_json import strict_model_validate_canonical_json

    root = Path(runtime_root)
    if root != root.resolve(strict=True) or Path(path) != root / "task-center-control.json":
        raise ValueError("task profile requires its exact original runtime path")
    raw = read_secure_regular_file(Path(path), expected_uid=os.geteuid(), expected_gid=os.getegid(), allowed_modes=frozenset({0o600}), max_bytes=16 * 1024)
    profile = strict_model_validate_canonical_json(TaskCenterControlProfile, raw)
    if profile.producer_commit != producer_commit or profile.runtime_root != root:
        raise ValueError("task profile code commit or runtime identity differs")
    return profile


def _validate_queue_profile(profile: TaskCenterControlProfile, *, actual: LabSchedulingQueueIdentity, claim_spool_root: Path) -> None:
    expected = profile.queue_identity
    def bound_store_id(identity: LabSchedulingQueueIdentity) -> str:
        return canonical_sha256({"canonical_path": identity.canonical_job_store_path, "database_generation": identity.database_generation,
            "application_id": identity.application_id, "schema_version": identity.schema_version, "implementation_digest": identity.implementation_digest})

    if expected is None or expected.store_id != bound_store_id(expected) or actual.store_id != bound_store_id(actual) or expected.model_dump(exclude={"schema_version", "store_id"}) != actual.model_dump(exclude={"schema_version", "store_id"}) or (expected.schema_version, actual.schema_version) not in ((16, 16), (16, 17), (17, 17)):
        raise ValueError("task profile original queue identity differs")
    root = Path(claim_spool_root)
    physical = root.lstat()
    from rquant.lab_job_protocol import LabCommandSpool
    LabCommandSpool._validate_private_directory_stat(physical, label="task profile claim spool")
    if root != root.resolve(strict=True) or root != profile.claim_spool_root or (physical.st_dev, physical.st_ino) != profile.claim_spool_generation:
        raise ValueError("task profile original claim spool identity differs")


def build_lab_scheduling_control(path: Path | None, *, store: object, producer_commit: str, runtime_root: Path,
                                 claim_spool_root: Path, maintenance_scope: LabSchedulingMaintenanceScope | None,
                                 production_mode: bool) -> LabSchedulingBarrierPort | None:
    if path is None:
        return None
    from rquant.lab_jobs import LabJobStore

    if type(store) is not LabJobStore:
        raise TypeError("task profile requires the original concrete Lab job store")
    profile = load_task_center_control_profile(path, producer_commit=producer_commit, runtime_root=runtime_root)
    if not profile.enabled:
        return None
    if production_mode:
        raise ValueError("task profile production installation and migration require separate authorization")
    actual = store.scheduling_identity()
    _validate_queue_profile(profile, actual=actual, claim_spool_root=claim_spool_root)
    if actual.schema_version == 16 and not profile.allow_local_migration:
        raise ValueError("task profile requires an explicit quiescent local migration")
    if profile.maintenance_scope != maintenance_scope or maintenance_scope is None:
        raise ValueError("task profile original maintenance scope differs")
    roots = (maintenance_scope.report_root, maintenance_scope.artifact_commit_root, maintenance_scope.final_artifact_root)
    if tuple((root.lstat().st_dev, root.lstat().st_ino) for root in roots) != profile.maintenance_generations:
        raise ValueError("task profile original maintenance generation differs")
    return LabSchedulingBarrierPort(claim_spool_root, store=store, maintenance_scope=maintenance_scope)


def task_center_worker_barrier_identity(path: Path | None, *, producer_commit: str, runtime_root: Path,
                                       claim_spool_root: Path, production_mode: bool) -> str | None:
    if path is None:
        return None
    from rquant.lab_jobs import LabJobReader

    profile = load_task_center_control_profile(path, producer_commit=producer_commit, runtime_root=runtime_root)
    if not profile.enabled:
        return None
    if production_mode:
        raise ValueError("task profile production worker installation requires separate authorization")
    state = LabJobReader(Path(profile.queue_identity.canonical_job_store_path)).scheduling_state()
    if state is None:
        raise ValueError("task worker requires the original migrated scheduler control")
    _validate_queue_profile(profile, actual=state.queue_identity, claim_spool_root=claim_spool_root)
    identity = canonical_sha256({"contract": "lab-scheduling-metadata-port/v1", "root": str(claim_spool_root),
        "root_generation": profile.claim_spool_generation, "queue_path": profile.queue_identity.canonical_job_store_path})
    if state.barrier_identity != identity:
        raise ValueError("task worker original barrier identity differs")
    return identity


class TaskUnitRunSource:
    def __init__(self, *, identity: TaskControlIdentity) -> None:
        self.identity = TaskControlIdentity.model_validate(identity)
        self.path = Path(self.identity.path)

    def _check_physical(self) -> None:
        try:
            physical = self.path.lstat()
        except OSError as exc:
            raise ValueError("original task journal is unavailable") from exc
        if not stat.S_ISREG(physical.st_mode) or physical.st_nlink != 1 or (physical.st_dev, physical.st_ino) != (self.identity.device, self.identity.inode):
            raise ValueError("original task journal physical identity differs")

    def read(self, *, host_name: str, boot_id: str, manifest_digest: str, units: tuple[str, ...], cutoff: datetime) -> tuple[TaskUnitRunEvidence, ...]:
        if cutoff.tzinfo is None or len(units) > 32 or len(set(units)) != len(units):
            raise ValueError("unit source requires a finite exact unit set and aware cutoff")
        self._check_physical()
        connection = sqlite3.connect("file:" + quote(os.fspath(self.path), safe="/") + "?mode=ro", uri=True, timeout=1)
        try:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            instance = connection.execute("SELECT instance_id FROM page_control_task_authority WHERE singleton=1").fetchone()
            if instance is None or instance[0] != self.identity.instance_id:
                raise ValueError("original task journal instance identity differs")
            indexes = connection.execute("SELECT unit,command_id,boot_id FROM page_control_unit_index ORDER BY unit LIMIT 33").fetchall()
            if len(indexes) > 32:
                raise ValueError("original unit source exceeds 32 entries")
            runs: list[TaskUnitRunEvidence] = []
            for unit, command_id, indexed_boot in indexes:
                row = connection.execute("SELECT CASE WHEN length(CAST(payload_json AS BLOB))<=32768 THEN payload_json END,length(CAST(payload_json AS BLOB)) FROM page_control_unit_run WHERE command_id=?", (command_id,)).fetchone()
                if row is None or row[1] > 32 * 1024:
                    raise ValueError("unit index has no bounded original effect")
                effect = TaskUnitEffect.model_validate_json(row[0])
                validate_task_journal_command(connection, effect.command, self.identity)
                command = effect.command
                if (command.unit, command.command_id, command.context.boot_id) != (unit, command_id, indexed_boot):
                    raise ValueError("original unit index identity differs")
                if unit not in units or (command.context.host_name, indexed_boot, command.context.manifest_digest) != (host_name, boot_id, manifest_digest):
                    continue
                if effect.updated_at > cutoff or effect.run is not None and effect.run.observed_at > cutoff:
                    raise ValueError("unit source contains facts after the cutoff")
                if effect.run is not None:
                    runs.append(effect.run)
            self._check_physical()
            connection.rollback()
            return tuple(runs)
        except sqlite3.Error as exc:
            raise ValueError("original task journal read is unavailable") from exc
        finally:
            connection.close()
