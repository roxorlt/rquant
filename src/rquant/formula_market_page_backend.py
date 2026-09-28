"""Trusted PageControl admission for an offline, source-bound formula market task."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pydantic import field_validator, model_validator

from rquant.page_control import SubmitFormulaMarketRun
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256, normalize_aware_utc
from rquant.screen.formula_history_projection import VerifiedFormulaHistoryProjection
from rquant.screen.formula_market_jobs import (
    FormulaMarketJobActiveError,
    FormulaMarketJobRequest,
    FormulaMarketJobStore,
)
from rquant.screen.formula_market_universe import peek_formula_market_universe_identity

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class FormulaMarketPageBackendConfig(RuntimeContractModel):
    """Four trusted local paths; browser commands cannot supply or override them."""

    universe_root: Path
    projection_root: Path
    state_path: Path
    artifact_directory: Path

    @field_validator("universe_root", "projection_root", "state_path", "artifact_directory")
    @classmethod
    def require_canonical_absolute_path(cls, path: Path) -> Path:
        if not path.is_absolute() or path != Path(os.path.abspath(path)):
            raise ValueError("formula task paths must be absolute and canonical")
        if path.resolve(strict=False) != path:
            raise ValueError("formula task paths must not follow symbolic links")
        return path

    @model_validator(mode="after")
    def validate_distinct_paths(self) -> FormulaMarketPageBackendConfig:
        roots = (self.universe_root, self.projection_root, self.artifact_directory)
        if len(set(roots)) != len(roots) or self.state_path.parent in roots:
            raise ValueError("formula task sources and state must use distinct directories")
        return self


class FormulaMarketPageBackend:
    """Bind current source identities and queue work; never evaluate the market here."""

    def __init__(
        self,
        config: FormulaMarketPageBackendConfig,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = FormulaMarketPageBackendConfig.model_validate(config)
        self.clock = clock or (lambda: datetime.now(UTC))
        if self.config.state_path.suffix != ".sqlite":
            raise ValueError("formula task state must be a .sqlite file")
        self.store = FormulaMarketJobStore(
            state_path=self.config.state_path,
            artifact_directory=self.config.artifact_directory,
            clock=self.clock,
        )
        observed = self.config.state_path.stat(follow_symlinks=False)
        if not stat.S_ISREG(observed.st_mode) or self.config.state_path.is_symlink():
            raise ValueError("formula task state must be a regular file")
        self._state_inode = (observed.st_dev, observed.st_ino)

    def _require_state_inode(self) -> None:
        observed = self.config.state_path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(observed.st_mode)
            or self.config.state_path.is_symlink()
            or (observed.st_dev, observed.st_ino) != self._state_inode
        ):
            raise ValueError("formula task state changed after backend initialization")

    @staticmethod
    def idempotency_key(command: SubmitFormulaMarketRun) -> str:
        return canonical_sha256(
            {"contract": "page-control-formula-market/v1", "command_id": command.command_id}
        )

    @staticmethod
    def _accepted(task_id: str) -> dict[str, str]:
        return {"outcome": "task_queued", "task_id": task_id}

    def recover(self, command: SubmitFormulaMarketRun) -> dict[str, str] | None:
        command = SubmitFormulaMarketRun.model_validate(command)
        self._require_state_inode()
        existing = self.store.admission_by_key(self.idempotency_key(command))
        if existing is None:
            return None
        request, task_id = existing
        if (
            request.formula != command.formula
            or request.trade_date != command.trade_date
            or request.universe_root != self.config.universe_root
            or request.projection_root != self.config.projection_root
        ):
            raise ValueError("formula command conflicts with the admitted task")
        return self._accepted(task_id)

    def submit(self, command: SubmitFormulaMarketRun) -> dict[str, str]:
        command = SubmitFormulaMarketRun.model_validate(command)
        recovered = self.recover(command)
        if recovered is not None:
            return recovered
        now = normalize_aware_utc(self.clock())
        if command.trade_date > now.astimezone(_SHANGHAI).date():
            raise ValueError("formula date is in the future")
        universe_identity = peek_formula_market_universe_identity(
            self.config.universe_root, command.trade_date
        )
        projection = VerifiedFormulaHistoryProjection(self.config.projection_root)
        current = projection.catalog()
        verified = projection.require_open_day(
            command.trade_date, expected_identity=current.identity
        )
        if verified.updated_at > now:
            raise ValueError("formula history is newer than the admission clock")
        if (
            peek_formula_market_universe_identity(
                self.config.universe_root, command.trade_date
            ) != universe_identity
            or projection.catalog().identity != current.identity
        ):
            raise ValueError("formula source changed during admission")
        self._require_state_inode()
        request = FormulaMarketJobRequest(
            idempotency_key=self.idempotency_key(command),
            formula=command.formula,
            trade_date=command.trade_date,
            decision_at=now,
            universe_root=self.config.universe_root,
            projection_root=self.config.projection_root,
            expected_universe_sha256=universe_identity,
            expected_projection_identity=verified.identity,
        )
        try:
            queued = self.store.submit(request)
        except FormulaMarketJobActiveError:
            return {"outcome": "task_conflict", "reason": "task_active"}
        return self._accepted(queued.task_id)
