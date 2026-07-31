"""Allow-listed runtime builder for one isolated live strategy runner."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pydantic import Field, StrictInt, field_validator

from rquant.feature_spool import FeatureBatchSpool
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.strategy_live_service import run_strategy_live_batch
from rquant.strategy_runner import StrategyEvaluator, StrategyRunnerStore
from rquant.strategy_spec import StrategySpec
from rquant.strict_json import strict_model_validate_json

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class StrategyLiveRuntimeSettings(RuntimeContractModel):
    feature_spool_root: Path
    runner_state_path: Path
    strategy_spec_path: Path
    strategy_id: str = Field(min_length=1)
    strategy_version: StrictInt = Field(ge=1)
    batch_limit: StrictInt = Field(default=128, ge=1)

    @field_validator(
        "feature_spool_root",
        "runner_state_path",
        "strategy_spec_path",
    )
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("strategy runtime data paths must be absolute")
        return value


@dataclass(frozen=True)
class StrategyEvaluatorBinding:
    """One process-local evaluator explicitly admitted by its caller."""

    strategy_id: str
    strategy_version: int
    contract_fingerprint: str
    evaluator: StrategyEvaluator

    def __post_init__(self) -> None:
        if not isinstance(self.strategy_id, str) or not self.strategy_id.strip():
            raise ValueError("evaluator strategy_id cannot be empty")
        if (
            not isinstance(self.strategy_version, int)
            or isinstance(self.strategy_version, bool)
            or self.strategy_version < 1
        ):
            raise ValueError("evaluator strategy_version must be positive")
        if _SHA256_PATTERN.fullmatch(self.contract_fingerprint) is None:
            raise ValueError("evaluator contract_fingerprint must be a SHA-256 digest")
        if not callable(self.evaluator):
            raise TypeError("evaluator must be callable")


StrategyEvaluatorLoader = Callable[[str, int], StrategyEvaluatorBinding]


def _read_frozen_strategy_spec(path: Path) -> StrategySpec:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(path, flags)
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError("strategy spec must be a regular file")
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            descriptor = -1
            payload = stream.read()
    except OSError as exc:
        raise ValueError("strategy spec is unavailable or contains a symlink") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    try:
        return strict_model_validate_json(StrategySpec, payload)
    except ValueError as exc:
        raise ValueError("strategy spec is invalid") from exc


def strategy_live_builder(
    *,
    evaluator_loader: StrategyEvaluatorLoader,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    """Build one stateful strategy step without dynamic imports or production I/O."""

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.STRATEGY_LIVE:
            raise ValueError("runtime service kind must be strategy_live")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("strategy-live service must run on the live plane")

        settings = StrategyLiveRuntimeSettings.model_validate(dict(manifest.settings))
        spec = _read_frozen_strategy_spec(settings.strategy_spec_path)
        if (
            spec.strategy_id != settings.strategy_id
            or spec.version != settings.strategy_version
        ):
            raise ValueError("strategy spec identity does not match runtime settings")
        if spec.producer_commit != manifest.producer_commit:
            raise ValueError("strategy spec producer commit does not match runtime manifest")

        binding = evaluator_loader(settings.strategy_id, settings.strategy_version)
        if not isinstance(binding, StrategyEvaluatorBinding):
            raise TypeError("evaluator loader must return StrategyEvaluatorBinding")
        if (
            binding.strategy_id != settings.strategy_id
            or binding.strategy_version != settings.strategy_version
        ):
            raise ValueError("evaluator identity does not match runtime settings")

        feature_spool = FeatureBatchSpool(settings.feature_spool_root)
        runner = StrategyRunnerStore(
            settings.runner_state_path,
            spec=spec,
            evaluator_contract_fingerprint=binding.contract_fingerprint,
        )

        def step() -> RuntimeStepResult:
            summary = run_strategy_live_batch(
                feature_spool=feature_spool,
                runner=runner,
                evaluator=binding.evaluator,
                observed_at=clock(),
                limit=settings.batch_limit,
            )
            backlog = max(
                0,
                summary.source_high_watermark - summary.last_feature_sequence,
            )
            return RuntimeStepResult(
                input_sequence=summary.last_feature_sequence,
                output_sequence=summary.runner_signal_high_watermark,
                processed_count=summary.processed_count,
                backlog_count=backlog,
                source_generations={
                    "feature_spool": summary.source_generation_id,
                    "runner_signal": runner.source_generation_id,
                },
            )

        return step

    return build


__all__ = [
    "StrategyEvaluatorBinding",
    "StrategyEvaluatorLoader",
    "StrategyLiveRuntimeSettings",
    "strategy_live_builder",
]
