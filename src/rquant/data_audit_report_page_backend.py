"""Trusted PageControl admission for bounded, read-only data audit reports."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import Field, field_validator, model_validator

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import capture_data_audit_replica_identity
from rquant.data_audit_report_jobs import DataAuditReportJobRequest, DataAuditReportJobStore
from rquant.page_control import SubmitDataAuditReport
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class DataAuditReportPageBackendConfig(RuntimeContractModel):
    """Trusted local source and policy; no field comes from the browser command."""

    primary_path: Path
    replica_path: Path
    state_path: Path
    report_directory: Path
    null_fields: tuple[DailyBarNullFieldSpec, ...] = Field(min_length=1, max_length=9)

    @field_validator("primary_path", "replica_path", "state_path", "report_directory")
    @classmethod
    def require_canonical_absolute_path(cls, path: Path) -> Path:
        if (
            not path.is_absolute()
            or path != Path(os.path.abspath(path))
            or path.parent.resolve(strict=False) != path.parent
        ):
            raise ValueError("audit backend paths must be absolute and canonical")
        return path

    @model_validator(mode="after")
    def validate_distinct_paths_and_fields(self) -> DataAuditReportPageBackendConfig:
        if len({self.primary_path, self.replica_path, self.state_path}) != 3:
            raise ValueError("audit primary, replica and task state paths must be distinct")
        if len({item.field_name for item in self.null_fields}) != len(self.null_fields):
            raise ValueError("audit NULL fields must be unique")
        return self


class DataAuditReportPageBackend:
    """Capture one O(1) sealed-replica identity, then enqueue without opening DuckDB."""

    def __init__(
        self,
        config: DataAuditReportPageBackendConfig,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = DataAuditReportPageBackendConfig.model_validate(config)
        self.clock = clock or (lambda: datetime.now(UTC))
        state = self.config.state_path
        if state.suffix != ".sqlite" or state.is_symlink():
            raise ValueError("audit task state must be a non-symlink .sqlite path")
        if state.exists():
            observed = state.stat(follow_symlinks=False)
            if not stat.S_ISREG(observed.st_mode):
                raise ValueError("audit task state must be a regular file")
            for source in (self.config.primary_path, self.config.replica_path):
                if source.exists() and os.path.samefile(state, source):
                    raise ValueError("audit task state cannot alias a source database")
        directory = self.config.report_directory
        if directory.is_symlink() or (directory.exists() and not directory.is_dir()):
            raise ValueError("audit report directory must be a real directory")
        self.store = DataAuditReportJobStore(
            state_path=state,
            report_directory=directory,
            clock=self.clock,
        )
        created_state = state.stat(follow_symlinks=False)
        if not stat.S_ISREG(created_state.st_mode) or state.is_symlink():
            raise ValueError("audit task state must remain a regular file")
        self._state_inode = (created_state.st_dev, created_state.st_ino)

    def _require_state_inode(self) -> tuple[int, int]:
        observed = self.config.state_path.stat(follow_symlinks=False)
        inode = (observed.st_dev, observed.st_ino)
        if (
            not stat.S_ISREG(observed.st_mode)
            or self.config.state_path.is_symlink()
            or inode != self._state_inode
        ):
            raise ValueError("audit task state changed after backend initialization")
        return inode

    @staticmethod
    def idempotency_key(command: SubmitDataAuditReport) -> str:
        return canonical_sha256(
            {"contract": "page-control-data-audit-report/v1", "command_id": command.command_id}
        )

    @staticmethod
    def _accepted(task_id: str) -> dict[str, str]:
        return {"outcome": "task_queued", "task_id": task_id}

    def recover(self, command: SubmitDataAuditReport) -> dict[str, str] | None:
        command = SubmitDataAuditReport.model_validate(command)
        self._require_state_inode()
        existing = self.store.admission_by_key(self.idempotency_key(command))
        if existing is None:
            return None
        request, task_id = existing
        if (
            request.audit_start != command.audit_start
            or request.observed_through != command.observed_through
        ):
            raise ValueError("data audit command conflicts with a prior task request")
        return self._accepted(task_id)

    def submit(self, command: SubmitDataAuditReport) -> dict[str, str]:
        command = SubmitDataAuditReport.model_validate(command)
        recovered = self.recover(command)
        if recovered is not None:
            return recovered
        now = self.clock()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("audit admission clock must be timezone-aware")
        local = now.astimezone(_SHANGHAI)
        closed_through = (
            local.date() if local.time() >= time(15) else local.date() - timedelta(days=1)
        )
        if command.observed_through > closed_through:
            raise ValueError("audit end date must be after Shanghai market close")
        try:
            state_inode = self._require_state_inode()
            primary = self.config.primary_path.stat(follow_symlinks=False)
            if not stat.S_ISREG(primary.st_mode) or self.config.primary_path.is_symlink():
                raise ValueError("trusted primary is unavailable")
            if (primary.st_dev, primary.st_ino) == state_inode:
                raise ValueError("trusted primary aliases audit task state")
            if any(
                os.path.lexists(f"{self.config.replica_path}{suffix}")
                for suffix in (".wal", ".shm")
            ):
                raise ValueError("trusted read-only replica has an unsealed sidecar")
            identity = capture_data_audit_replica_identity(
                self.config.primary_path, self.config.replica_path
            )
            if (identity.device, identity.inode) == state_inode:
                raise ValueError("trusted read-only replica aliases audit task state")
            self._require_state_inode()
        except (OSError, ValueError) as exc:
            raise ValueError("trusted read-only replica is unavailable") from exc
        request = DataAuditReportJobRequest(
            idempotency_key=self.idempotency_key(command),
            primary_path=self.config.primary_path,
            replica_path=self.config.replica_path,
            replica_file_identity=identity,
            audit_start=command.audit_start,
            observed_through=command.observed_through,
            null_fields=self.config.null_fields,
        )
        return self._accepted(self.store.submit(request).task_id)
