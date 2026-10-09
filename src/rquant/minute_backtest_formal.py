"""Original Lab/Experiment identities for the separate formal minute wrapper."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator

from rquant.definition_registry import ImmutableDefinitionRegistry, StrategySpecRegistration
from rquant.experiment_registry import DateRange, ExperimentRegistry, ExperimentSpec, FormalExperimentPlan, HypothesisFamilyManifest
from rquant.lab_job_center import ResearchJobSubmission, _research_parameter, build_research_job_submission
from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile, MinuteReplayModel
from rquant.minute_backtest_formal_adapter import MinuteFormalParameters, MinuteFormalRunInput
from rquant.minute_backtest_producer import MinuteReplayCatalog, PublishedMinuteInput
from rquant.minute_backtest_publication_contracts import FrozenMinuteResearchInput
from rquant.research_run_spec import DatasetSnapshotIdentity, ResearchRunParameters, ResourceClass
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256
from rquant.strategy_job_adapters import build_adapter_execution_contract


class MinuteExperimentProtocol(MinuteReplayModel):
    train_range: DateRange
    validation_range: DateRange
    frozen_outer_test_range: DateRange

    @model_validator(mode="after")
    def chronological(self) -> MinuteExperimentProtocol:
        if not self.train_range.end_date < self.validation_range.start_date <= self.validation_range.end_date < self.frozen_outer_test_range.start_date:
            raise ValueError("minute experiment ranges must preserve original chronological separation")
        return self


class PreparedMinuteRequest(MinuteReplayModel):
    kind: Literal["minute_runtime_replay"] = "minute_runtime_replay"
    frozen: FrozenMinuteResearchInput
    published: PublishedMinuteInput
    catalog: MinuteReplayCatalog
    registration: StrategySpecRegistration
    formal_plan: FormalExperimentPlan
    deadline: AwareUtcDatetime
    random_seed: int = Field(default=0, ge=0)
    resource_class: Literal[ResourceClass.STANDARD] = ResourceClass.STANDARD
    max_attempts: int = Field(default=2, ge=1, le=5)

    def submission(self, *, job_id: UUID) -> ResearchJobSubmission:
        if self.frozen != self.published.receipt.frozen or self.registration != self.frozen.wrapper_registration:
            raise PermissionError("minute prepared complete source/registration changed")
        expected = self.catalog.resolve(source_key=self.frozen.runtime.source_key, source_version=self.frozen.runtime.source_version,
            owner_id=self.frozen.runtime.owner_id)
        if expected != self.published.receipt:
            raise PermissionError("minute prepared publication differs from installed independent source")
        return build_research_job_submission(MinuteFormalRunInput.from_frozen(self.frozen),
            gate_decision=self.published.gate_decision, code_sha=self.frozen.runtime.producer_commit,
            dataset_snapshot=self.published.identity, feature_contract=build_adapter_execution_contract(
                "minute-runtime-replay", "2", self.frozen.runtime.producer_commit),
            execution_costs=self.frozen.runtime.execution_profile.execution_costs, random_seed=self.random_seed,
            resource_class=self.resource_class, deadline=self.deadline, job_id=job_id, max_attempts=self.max_attempts,
            trusted_strategy_registration=self.registration, formal_experiment_plan=self.formal_plan, minute_catalog=self.catalog)


def _minute_formal_parameters(run: MinuteFormalRunInput) -> ResearchRunParameters:
    return ResearchRunParameters(strategy_name=run.kind, start_date=run.start_date, end_date=run.end_date,
        arguments=tuple(_research_parameter(name, getattr(run.parameters, name))
            for name in type(run.parameters).model_fields
            if name != "prepared_publication_json" or getattr(run.parameters, name) is not None))


def _build_minute_formal_plan(*, registration: StrategySpecRegistration,
    identity: DatasetSnapshotIdentity, parameters: ResearchRunParameters,
    execution_profile: MinuteReplayExecutionProfile, producer_commit: str, full_input_hash: str,
    protocol: MinuteExperimentProtocol, now: datetime, random_seed: int,
    adapter_id: str, adapter_version: str, family_prefix: str, hypothesis_variant: str,
    family_id: str | None = None,
) -> FormalExperimentPlan:
    contract = build_adapter_execution_contract(adapter_id, adapter_version, producer_commit)
    family = family_id or family_prefix + canonical_sha256({"input": full_input_hash, "protocol": protocol})
    spec = ExperimentSpec(strategy_spec_fingerprint=registration.spec.spec_fingerprint,
        strategy_executable_fingerprint=registration.executable_fingerprint,
        candidate_schema_fingerprint=registration.candidate_schema_fingerprint,
        dataset_snapshot_id=identity.snapshot_id, code_commit=producer_commit,
        parameter_fingerprint=canonical_sha256(parameters), hypothesis_family=family,
        metric_definition_fingerprint=canonical_sha256({"contract": "minute-native-performance/v2",
            "basis": "pit_asof_15:00", "profile_hash": execution_profile.profile_hash,
            "overfit": "not_evaluated"}), train_range=protocol.train_range, validation_range=protocol.validation_range,
        frozen_outer_test_range=protocol.frozen_outer_test_range,
        cost_model_fingerprint=canonical_sha256(execution_profile.execution_costs),
        execution_model_fingerprint=canonical_sha256({"contract": "lab-adapter-execution/v1",
            "adapter_id": adapter_id, "adapter_version": adapter_version, "feature_contract": contract}), seed=random_seed)
    return FormalExperimentPlan(schema_version=2, spec=spec, hypothesis_variant=hypothesis_variant,
        strategy_definition_fingerprint=registration.fingerprint, definition_registration_record_hash=registration.record_hash,
        preregistered_at=now)


def build_minute_plan(value: FrozenMinuteResearchInput, published: PublishedMinuteInput, *, catalog: MinuteReplayCatalog,
    definitions: ImmutableDefinitionRegistry, protocol: MinuteExperimentProtocol, now: datetime, deadline: datetime,
    random_seed: int = 0, family_id: str | None = None, hypothesis_variant: str = "native-minute-runtime",
) -> PreparedMinuteRequest:
    value = FrozenMinuteResearchInput.model_validate(value.model_dump(mode="python"))
    if published.receipt.frozen != value or not published.gate_decision.allowed:
        raise PermissionError("minute complete publication differs from original prepared source")
    if now < value.provenance.published_at or deadline <= now:
        raise PermissionError("minute formal preregistration/publication/deadline times differ")
    registration = definitions.latest_strategy_spec("minute_runtime_replay", as_of=now)
    if registration != value.wrapper_registration:
        raise PermissionError("minute installed trusted wrapper registration differs")
    native = definitions.read_strategy_spec(value.runtime.strategy.registration_fingerprint, as_of=now)
    if native != value.native_registration:
        raise PermissionError("minute installed complete native registration differs")
    run = MinuteFormalRunInput.from_frozen(value)
    plan = _build_minute_formal_plan(registration=registration, identity=published.identity,
        parameters=_minute_formal_parameters(run), execution_profile=value.runtime.execution_profile,
        producer_commit=value.runtime.producer_commit, full_input_hash=value.full_input_hash, protocol=protocol,
        now=now, random_seed=random_seed, adapter_id="minute-runtime-replay", adapter_version="2",
        family_prefix="minute-runtime:", family_id=family_id, hypothesis_variant=hypothesis_variant)
    result = PreparedMinuteRequest(frozen=value, published=published, catalog=catalog, registration=registration,
        formal_plan=plan, deadline=deadline, random_seed=random_seed)
    result.submission(job_id=UUID(int=0))
    return result


def register_minute_plan(value: FrozenMinuteResearchInput, published: PublishedMinuteInput, *, catalog: MinuteReplayCatalog,
    definitions: ImmutableDefinitionRegistry, experiments: ExperimentRegistry, protocol: MinuteExperimentProtocol,
    now: datetime, deadline: datetime, random_seed: int = 0,
) -> PreparedMinuteRequest:
    prepared = build_minute_plan(value, published, catalog=catalog, definitions=definitions, protocol=protocol,
        now=now, deadline=deadline, random_seed=random_seed)
    plan = prepared.formal_plan
    experiments.register_formal_plan(plan, family_manifest=HypothesisFamilyManifest(
        hypothesis_family=plan.spec.hypothesis_family, experiment_ids=(plan.spec.experiment_id,),
        search_space_fingerprint=canonical_sha256(MinuteFormalParameters.from_frozen(value)),
        metric_definition_fingerprint=plan.spec.metric_definition_fingerprint, preregistered_at=now))
    return prepared
