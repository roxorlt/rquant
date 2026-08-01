"""Allow-listed runtime builder for one isolated live strategy runner."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pydantic import Field, StrictInt, field_validator

from rquant.feature_spool import FeatureBatchSpool
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
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
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_PRIVATE_FILE_MODE = 0o600


class StrategyLiveRuntimeSettings(RuntimeContractModel):
    feature_spool_root: Path
    runner_state_path: Path
    strategy_spec_path: Path
    strategy_spec_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_snapshot_root: Path
    candidate_max_age_seconds: StrictInt = Field(gt=0)
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
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("strategy runtime data paths must be absolute and normalized")
        return value

    @field_validator("candidate_snapshot_root")
    @classmethod
    def require_normalized_candidate_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("candidate snapshot root must be absolute")
        if value != Path(os.path.abspath(value)):
            raise ValueError("candidate snapshot root must be normalized without traversal")
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


def _load_builtin_evaluator(
    *,
    spec: StrategySpec,
    producer_commit: str,
) -> StrategyEvaluatorBinding:
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit)
    definition = registry.load_definition(spec.strategy_id, spec.version)
    if definition.spec != spec:
        raise ValueError("frozen strategy spec does not match built-in strategy spec")
    return registry.load_binding(spec.strategy_id, spec.version)


def _same_file_identity(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        left.st_mode,
        left.st_uid,
        left.st_nlink,
    ) == (
        right.st_dev,
        right.st_ino,
        right.st_mode,
        right.st_uid,
        right.st_nlink,
    )


def _same_file_version(left: os.stat_result, right: os.stat_result) -> bool:
    return _same_file_identity(left, right) and (
        left.st_size,
        left.st_mtime_ns,
        left.st_ctime_ns,
    ) == (
        right.st_size,
        right.st_mtime_ns,
        right.st_ctime_ns,
    )


def _read_frozen_strategy_spec(path: Path, *, expected_sha256: str) -> StrategySpec:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ValueError("strategy spec path is unsafe")
    parent_descriptor = -1
    child_descriptor = -1
    file_descriptor = -1
    try:
        parent_descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
        for component in path.parts[1:-1]:
            before = os.stat(
                component,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
            if stat.S_ISLNK(before.st_mode):
                raise ValueError("strategy spec path contains a symlink")
            child_descriptor = os.open(
                component,
                _DIRECTORY_FLAGS,
                dir_fd=parent_descriptor,
            )
            opened = os.fstat(child_descriptor)
            active = os.stat(
                component,
                dir_fd=parent_descriptor,
                follow_symlinks=False,
            )
            if not stat.S_ISDIR(opened.st_mode):
                raise ValueError("strategy spec parent is unsafe")
            if not _same_file_identity(before, opened) or not _same_file_identity(opened, active):
                raise ValueError("strategy spec parent identity changed")
            os.close(parent_descriptor)
            parent_descriptor = child_descriptor
            child_descriptor = -1

        name = path.name
        before = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode):
            raise ValueError("strategy spec cannot be a symlink")
        file_descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent_descriptor,
        )
        opened = os.fstat(file_descriptor)
        active = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if not _same_file_identity(before, opened) or not _same_file_identity(opened, active):
            raise ValueError("strategy spec identity changed")
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError("strategy spec must be a regular file")
        if opened.st_uid != os.getuid():
            raise ValueError("strategy spec must be owned by the current uid")
        if opened.st_nlink != 1:
            raise ValueError("strategy spec hardlink count must be one")
        if stat.S_IMODE(opened.st_mode) != _PRIVATE_FILE_MODE:
            raise ValueError("strategy spec permissions must be 0600")
        with os.fdopen(file_descriptor, "rb", closefd=False) as stream:
            payload = stream.read()
        after = os.fstat(file_descriptor)
        current = os.stat(name, dir_fd=parent_descriptor, follow_symlinks=False)
        if not _same_file_version(opened, after) or not _same_file_version(after, current):
            raise ValueError("strategy spec identity changed while being read")
    except OSError as exc:
        raise ValueError("strategy spec is unavailable or contains an unsafe symlink") from exc
    finally:
        if file_descriptor >= 0:
            os.close(file_descriptor)
        if child_descriptor >= 0:
            os.close(child_descriptor)
        if parent_descriptor >= 0:
            os.close(parent_descriptor)
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("strategy spec content does not match frozen SHA-256")
    try:
        return strict_model_validate_json(StrategySpec, payload)
    except ValueError as exc:
        raise ValueError("strategy spec is invalid") from exc


def strategy_live_builder(
    *,
    clock: Callable[[], datetime],
    evaluator_loader: StrategyEvaluatorLoader | None = None,
) -> RuntimeServiceBuilder:
    """Build one stateful strategy step without dynamic imports or production I/O."""

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.STRATEGY_LIVE:
            raise ValueError("runtime service kind must be strategy_live")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("strategy-live service must run on the live plane")

        settings = StrategyLiveRuntimeSettings.model_validate(dict(manifest.settings))
        spec = _read_frozen_strategy_spec(
            settings.strategy_spec_path,
            expected_sha256=settings.strategy_spec_sha256,
        )
        if spec.strategy_id != settings.strategy_id or spec.version != settings.strategy_version:
            raise ValueError("strategy spec identity does not match runtime settings")
        if spec.producer_commit != manifest.producer_commit:
            raise ValueError("strategy spec producer commit does not match runtime manifest")

        if evaluator_loader is None:
            binding = _load_builtin_evaluator(
                spec=spec,
                producer_commit=manifest.producer_commit,
            )
        else:
            binding = evaluator_loader(settings.strategy_id, settings.strategy_version)
        if not isinstance(binding, StrategyEvaluatorBinding):
            raise TypeError("evaluator loader must return StrategyEvaluatorBinding")
        if (
            binding.strategy_id != settings.strategy_id
            or binding.strategy_version != settings.strategy_version
        ):
            raise ValueError("evaluator identity does not match runtime settings")

        feature_spool = FeatureBatchSpool(settings.feature_spool_root)
        candidate_universe_loader = RuntimeCandidateUniverseLoader(
            RuntimeCandidateUniverseConfig(
                expected_commit=manifest.producer_commit,
                authorities=(
                    CandidateUniverseAuthority(
                        strategy_id=spec.strategy_id,
                        strategy_version=str(spec.version),
                        snapshot_root=settings.candidate_snapshot_root,
                        required=True,
                        max_age_seconds=settings.candidate_max_age_seconds,
                    ),
                ),
            )
        )
        runner = StrategyRunnerStore(
            settings.runner_state_path,
            spec=spec,
            evaluator_contract_fingerprint=binding.contract_fingerprint,
        )

        def step() -> RuntimeStepResult:
            summary = run_strategy_live_batch(
                feature_spool=feature_spool,
                candidate_universe_loader=candidate_universe_loader,
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
