"""Trusted PageControl adapter for read-only backfill plan task admission."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from pydantic import Field, field_validator, model_validator

from rquant.backfill_plan_artifact import capture_backfill_snapshot_identity
from rquant.backfill_plan_core import BackfillEstimateAssumptions
from rquant.backfill_plan_jobs import BackfillPlanJobRequest, BackfillPlanJobStore
from rquant.page_control import SubmitBackfillPlan
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256


class BackfillPlanPageBackendConfig(RuntimeContractModel):
    """Local trusted paths and assumptions; never populated from a browser command."""

    primary_path: Path
    replica_path: Path
    state_path: Path
    plan_directory: Path
    evidence_code_revision: str = Field(min_length=1, max_length=80)
    assumptions: BackfillEstimateAssumptions

    @field_validator("primary_path", "replica_path", "state_path", "plan_directory")
    @classmethod
    def require_absolute_path(cls, path: Path) -> Path:
        if not path.is_absolute():
            raise ValueError("backfill plan backend paths must be absolute")
        return path

    @model_validator(mode="after")
    def validate_distinct_paths(self) -> BackfillPlanPageBackendConfig:
        if not self.evidence_code_revision.strip():
            raise ValueError("evidence code revision must be non-empty")
        if len({self.primary_path, self.replica_path, self.state_path}) != 3:
            raise ValueError("primary, replica and task state must use distinct paths")
        return self


class BackfillPlanPageBackend:
    """Bind one PageControl command to a fixed local source without opening DuckDB."""

    def __init__(
        self,
        config: BackfillPlanPageBackendConfig,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = BackfillPlanPageBackendConfig.model_validate(config)
        self.clock = clock or (lambda: datetime.now(UTC))
        if self.config.state_path.is_symlink():
            raise ValueError("backfill plan task state cannot be a symlink")
        for source in (self.config.primary_path, self.config.replica_path):
            if (
                source.exists()
                and self.config.state_path.exists()
                and os.path.samefile(source, self.config.state_path)
            ):
                raise ValueError("backfill plan task state cannot alias a source database")
        self.store = BackfillPlanJobStore(
            state_path=self.config.state_path,
            plan_directory=self.config.plan_directory,
            clock=self.clock,
        )

    @staticmethod
    def idempotency_key(command: SubmitBackfillPlan) -> str:
        return canonical_sha256(
            {"contract": "page-control-backfill-plan/v1", "command_id": command.command_id}
        )

    @staticmethod
    def _accepted(task_id: str) -> dict[str, str]:
        return {"outcome": "task_queued", "task_id": task_id}

    def recover(self, command: SubmitBackfillPlan) -> dict[str, str] | None:
        existing = self.store.admission_by_key(self.idempotency_key(command))
        if existing is None:
            return None
        request, task_id = existing
        if (
            request.audit_start != command.audit_start
            or request.completed_through != command.completed_through
            or (request.owner is not None and request.owner!=command.actor_id)
            or (request.page_command_id is not None and request.page_command_id!=command.command_id)
        ):
            raise ValueError("backfill plan command conflicts with a prior task request")
        return self._accepted(task_id)

    def submit(self, command: SubmitBackfillPlan) -> dict[str, str]:
        recovered = self.recover(command)
        if recovered is not None:
            return recovered
        try:
            primary = self.config.primary_path.stat(follow_symlinks=False)
            if not stat.S_ISREG(primary.st_mode) or self.config.primary_path.is_symlink():
                raise ValueError("trusted primary is unavailable")
            identity = capture_backfill_snapshot_identity(self.config.replica_path)
        except (OSError, ValueError) as exc:
            raise ValueError("trusted read-only replica is unavailable") from exc
        if (identity.device, identity.inode) == (primary.st_dev, primary.st_ino):
            raise ValueError("trusted read-only replica aliases the primary")
        request = BackfillPlanJobRequest(
            idempotency_key=self.idempotency_key(command),
            snapshot_path=self.config.replica_path,
            snapshot_file_identity=identity,
            snapshot_label=f"replica-{canonical_sha256(identity.model_dump(mode='json'))[:24]}",
            evidence_code_revision=self.config.evidence_code_revision,
            audit_start=command.audit_start,
            completed_through=command.completed_through,
            observed_at=self.clock(),
            assumptions=self.config.assumptions,
            owner=command.actor_id,
            page_command_id=command.command_id,
        )
        return self._accepted(self.store.submit(request).task_id)
