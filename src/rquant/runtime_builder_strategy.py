"""Allow-listed runtime builder for one isolated live strategy runner."""

from __future__ import annotations

import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pydantic import Field, StrictInt, field_validator, model_validator

from rquant.definition_registry import (
    DefinitionExecutableIntegrityError,
    ImmutableDefinitionRegistry,
)
from rquant.feature_spool import FeatureBatchSpool
from rquant.runtime_candidate_universe import (
    CandidateUniverseAuthority,
    RuntimeCandidateUniverseConfig,
    RuntimeCandidateUniverseLoader,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_market_session import load_market_calendar_authority
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.runtime_shadow_validation import CompletionAttestationSigner
from rquant.signal_router_runtime import ReadonlySignalRouteAuthority
from rquant.strategy_live_service import (
    StrategyCompletionAttestationConfig,
    run_strategy_live_batch,
)
from rquant.strategy_paper_lifecycle import PaperBrokerLifecycleReader
from rquant.strategy_runner import StrategyEvaluator, StrategyRunnerStore

_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class StrategyLiveRuntimeSettings(RuntimeContractModel):
    feature_spool_root: Path
    runner_state_path: Path
    definition_registry_root: Path
    strategy_registration_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_spec_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    evaluator_contract_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    strategy_executable_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_schema_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_snapshot_root: Path
    paper_broker_path: Path
    paper_account_id: str = Field(min_length=1)
    candidate_max_age_seconds: StrictInt = Field(gt=0)
    strategy_id: str = Field(min_length=1)
    strategy_version: StrictInt = Field(ge=1)
    batch_limit: StrictInt = Field(default=128, ge=1)
    calendar_path: Path | None = None
    calendar_expected_commit: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{40}$",
    )
    calendar_content_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    signal_bus_path: Path | None = None
    routing_policy_fingerprint: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    producer_instance_id: str | None = Field(default=None, min_length=1)
    producer_version: str | None = Field(default=None, min_length=1)

    @field_validator(
        "feature_spool_root",
        "runner_state_path",
        "definition_registry_root",
        "paper_broker_path",
        "calendar_path",
        "signal_bus_path",
    )
    @classmethod
    def require_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
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

    @model_validator(mode="after")
    def validate_completion_authority(self) -> StrategyLiveRuntimeSettings:
        authority = (
            self.calendar_path,
            self.calendar_expected_commit,
            self.calendar_content_sha256,
            self.signal_bus_path,
            self.routing_policy_fingerprint,
            self.producer_instance_id,
            self.producer_version,
            self.strategy_spec_fingerprint,
            self.evaluator_contract_fingerprint,
        )
        if any(value is not None for value in authority) and not all(
            value is not None for value in authority
        ):
            raise ValueError("strategy completion authority must be configured as one group")
        return self

    @property
    def has_completion_authority(self) -> bool:
        return self.calendar_path is not None


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


def _require_production_completion_signer(
    signer: CompletionAttestationSigner | None,
    *,
    active_key_id: str | None,
) -> CompletionAttestationSigner:
    from rquant.runtime_shadow_validation import Ed25519CompletionAttestationSigner

    if signer is None:
        raise ValueError("strategy completion authority requires an attestation signer")
    if not isinstance(signer, Ed25519CompletionAttestationSigner):
        raise ValueError("strategy completion authority requires an Ed25519 attestation signer")
    if active_key_id is None or not active_key_id.strip():
        raise ValueError("strategy completion authority requires an active key id")
    if signer.key_id != active_key_id:
        raise ValueError("strategy completion signer must use the active key id")
    return signer


def strategy_live_builder(
    *,
    clock: Callable[[], datetime],
    evaluator_loader: StrategyEvaluatorLoader | None = None,
    completion_attestation_signer: CompletionAttestationSigner | None = None,
    completion_attestation_active_key_id: str | None = None,
) -> RuntimeServiceBuilder:
    """Build one stateful strategy step without dynamic imports or production I/O."""

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.STRATEGY_LIVE:
            raise ValueError("runtime service kind must be strategy_live")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("strategy-live service must run on the live plane")
        if evaluator_loader is not None:
            raise TypeError("arbitrary strategy evaluator loaders are not trusted")

        settings = StrategyLiveRuntimeSettings.model_validate(dict(manifest.settings))
        from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

        builtin_registry = BuiltinStrategyEvaluatorRegistry(
            producer_commit=manifest.producer_commit
        )
        execution_registry = builtin_registry.trusted_executable_registry()
        definition_registry = ImmutableDefinitionRegistry(
            settings.definition_registry_root,
            execution_registry=execution_registry,
        )
        build_at = clock()
        try:
            registration = definition_registry.read_strategy_spec(
                settings.strategy_registration_fingerprint,
                as_of=build_at,
            )
        except DefinitionExecutableIntegrityError as exc:
            raise ValueError(
                "published strategy evaluator fingerprint no longer matches the trusted graph"
            ) from exc
        if registration is None:
            raise ValueError("published strategy registration is unavailable")
        spec = registration.spec
        if spec.strategy_id != settings.strategy_id or spec.version != settings.strategy_version:
            raise ValueError("strategy registration identity does not match runtime settings")
        if spec.producer_commit != manifest.producer_commit:
            raise ValueError("strategy registration producer commit does not match manifest")
        feature_registration = definition_registry.read_feature_contract(
            registration.feature_contract_fingerprint,
            as_of=build_at,
        )
        if feature_registration is None:
            raise ValueError("published feature contract registration is unavailable")

        definition = builtin_registry.load_definition(spec.strategy_id, spec.version)
        if definition.spec.spec_fingerprint != spec.spec_fingerprint:
            raise ValueError("published strategy spec does not match built-in strategy spec")
        if (
            settings.strategy_spec_fingerprint is not None
            and settings.strategy_spec_fingerprint != spec.spec_fingerprint
        ):
            raise ValueError("strategy spec fingerprint does not match runtime binding")
        if (
            registration.candidate_schema_fingerprint != settings.candidate_schema_fingerprint
            or definition.candidate_schema_fingerprint != settings.candidate_schema_fingerprint
        ):
            raise ValueError("candidate schema fingerprint does not match runtime binding")
        binding = builtin_registry.load_binding(spec.strategy_id, spec.version)
        if not isinstance(binding, StrategyEvaluatorBinding):
            raise TypeError("built-in evaluator registry returned an invalid binding")
        if (
            binding.strategy_id != settings.strategy_id
            or binding.strategy_version != settings.strategy_version
        ):
            raise ValueError("evaluator identity does not match runtime settings")
        if (
            binding.contract_fingerprint != registration.executable_fingerprint
            or definition.executable_fingerprint != registration.executable_fingerprint
            or settings.strategy_executable_fingerprint != registration.executable_fingerprint
            or (
                settings.evaluator_contract_fingerprint is not None
                and settings.evaluator_contract_fingerprint != binding.contract_fingerprint
            )
        ):
            raise ValueError("built-in evaluator fingerprint does not match published registration")

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
                        definition_fingerprint=registration.fingerprint,
                        executable_fingerprint=settings.strategy_executable_fingerprint,
                        candidate_schema_fingerprint=settings.candidate_schema_fingerprint,
                        static_feature_names=tuple(sorted(definition.static_feature_schema)),
                        static_feature_schema={
                            name: semantic.contract_payload()
                            for name, semantic in definition.static_feature_schema.items()
                        },
                    ),
                ),
            )
        )
        runner = StrategyRunnerStore(
            settings.runner_state_path,
            spec=spec,
            evaluator_contract_fingerprint=binding.contract_fingerprint,
            feature_contract=feature_registration.contract,
            lifecycle_feature_source=PaperBrokerLifecycleReader(
                settings.paper_broker_path,
                account_id=settings.paper_account_id,
            ),
        )
        calendar = None
        route_authority = None
        completion_attestation = None
        if settings.has_completion_authority:
            completion_signer = _require_production_completion_signer(
                completion_attestation_signer,
                active_key_id=completion_attestation_active_key_id,
            )
            assert settings.calendar_path is not None
            assert settings.calendar_expected_commit is not None
            assert settings.calendar_content_sha256 is not None
            assert settings.signal_bus_path is not None
            assert settings.routing_policy_fingerprint is not None
            calendar = load_market_calendar_authority(
                settings.calendar_path,
                expected_commit=settings.calendar_expected_commit,
            )
            if calendar.content_sha256 != settings.calendar_content_sha256:
                raise ValueError("strategy calendar content identity does not match settings")
            route_authority = ReadonlySignalRouteAuthority(
                path=settings.signal_bus_path,
                expected_routing_policy_fingerprint=(settings.routing_policy_fingerprint),
            )
            completion_attestation = StrategyCompletionAttestationConfig(
                signer=completion_signer,
                strategy_registration_fingerprint=registration.fingerprint,
                executable_fingerprint=registration.executable_fingerprint,
                candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
                feature_registration_fingerprint=feature_registration.fingerprint,
                feature_contract_fingerprint=(feature_registration.contract.contract_fingerprint),
                producer_manifest_fingerprint=manifest.manifest_fingerprint,
            )

        def step() -> RuntimeStepResult:
            summary = run_strategy_live_batch(
                feature_spool=feature_spool,
                candidate_universe_loader=candidate_universe_loader,
                runner=runner,
                evaluator=binding.evaluator,
                observed_at=clock(),
                limit=settings.batch_limit,
                calendar=calendar,
                route_authority=route_authority,
                completion_source_id=(
                    manifest.service_id if settings.has_completion_authority else None
                ),
                producer_service_id=(
                    manifest.service_id if settings.has_completion_authority else None
                ),
                producer_instance_id=settings.producer_instance_id,
                producer_version=settings.producer_version,
                completion_attestation=completion_attestation,
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
