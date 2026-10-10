"""Full parameter publications through the original Lab and Experiment identities."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field

from rquant.definition_registry import ImmutableDefinitionRegistry, StrategySpecRegistration
from rquant.experiment_registry import ExperimentRegistry, FormalExperimentPlan, HypothesisFamilyManifest
from rquant.lab_job_center import ResearchJobSubmission, build_research_job_submission
from rquant.minute_backtest_contracts import MinuteReplayModel
from rquant.minute_backtest_formal import (
    MinuteExperimentProtocol, _build_minute_formal_plan, _minute_formal_parameters,
)
from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalParameters, MinuteParameterFormalRunInput
from rquant.minute_backtest_parameter_contracts import FrozenMinuteParameterResearchInput
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
from rquant.minute_backtest_parameter_producer import (
    MinuteParameterPreparedPublication, MinuteParameterReplayCatalog, PublishedMinuteParameterInput,
)
from rquant.research_run_spec import ResourceClass
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256
from rquant.strategy_job_adapters import build_adapter_execution_contract


class PreparedMinuteParameterRequest(MinuteReplayModel):
    kind: Literal["minute_parameter_replay"] = "minute_parameter_replay"
    frozen: FrozenMinuteParameterResearchInput
    published: PublishedMinuteParameterInput
    prepared_publication: MinuteParameterPreparedPublication
    catalog: MinuteParameterReplayCatalog
    registration: StrategySpecRegistration
    formal_plan: FormalExperimentPlan
    deadline: AwareUtcDatetime
    random_seed: int = Field(default=0, ge=0, lt=2**63)
    resource_class: Literal[ResourceClass.STANDARD] = ResourceClass.STANDARD
    max_attempts: int = Field(default=2, ge=1, le=5)

    @property
    def study_binding(self) -> MinuteParameterStudyBinding | None:
        return self.frozen.runtime.study_binding

    @property
    def run_input(self) -> MinuteParameterFormalRunInput:
        return MinuteParameterFormalRunInput(start_date=self.frozen.runtime.start_date,
            end_date=self.frozen.runtime.end_date, parameters=MinuteParameterFormalParameters.from_prepared(
                self.frozen, self.prepared_publication))

    def submission(self, *, job_id: UUID) -> ResearchJobSubmission:
        if self.frozen != self.published.receipt.frozen or self.registration != self.frozen.wrapper_registration:
            raise PermissionError("parameter prepared complete source/registration changed")
        if self.catalog.resolve_prepared(self.prepared_publication) != self.published.receipt:
            raise PermissionError("parameter prepared full publication differs from installed independent facts")
        return build_research_job_submission(self.run_input,
            gate_decision=self.published.gate_decision, code_sha=self.frozen.runtime.producer_commit,
            dataset_snapshot=self.published.identity,
            feature_contract=build_adapter_execution_contract("minute-parameter-replay", "1", self.frozen.runtime.producer_commit),
            execution_costs=self.frozen.runtime.execution_profile.execution_costs, random_seed=self.random_seed,
            resource_class=self.resource_class, deadline=self.deadline, job_id=job_id, max_attempts=self.max_attempts,
            trusted_strategy_registration=self.registration, formal_experiment_plan=self.formal_plan,
            parameter_catalog=self.catalog)


def build_minute_parameter_plan(value: FrozenMinuteParameterResearchInput,
    published: PublishedMinuteParameterInput, *, prepared_publication: MinuteParameterPreparedPublication,
    catalog: MinuteParameterReplayCatalog, definitions: ImmutableDefinitionRegistry,
    protocol: MinuteExperimentProtocol, now: datetime, deadline: datetime,
    random_seed: int = 0, family_id: str | None = None,
    hypothesis_variant: str = "minute-parameters-semantic2",
) -> PreparedMinuteParameterRequest:
    value = FrozenMinuteParameterResearchInput.model_validate(value.model_dump(mode="python"))
    if published.receipt.frozen != value or not published.gate_decision.allowed:
        raise PermissionError("parameter complete publication differs from original prepared source")
    if now < value.provenance.published_at or deadline <= now:
        raise PermissionError("parameter formal preregistration/publication/deadline times differ")
    for window in (protocol.train_range, protocol.validation_range, protocol.frozen_outer_test_range):
        if not value.runtime.start_date <= window.start_date <= window.end_date <= value.runtime.end_date:
            raise PermissionError("parameter protocol exceeds its full installed source range")
    study = value.runtime.study_binding
    if study is not None and ((study.train_range, study.validation_range, study.frozen_outer_test_range,
            study.protocol.random_seed) != (protocol.train_range, protocol.validation_range,
            protocol.frozen_outer_test_range, random_seed)):
        raise PermissionError("parameter formal plan differs from its complete three-part study/seed")
    registration = definitions.latest_strategy_spec("minute_parameter_replay", as_of=now)
    if registration != value.wrapper_registration:
        raise PermissionError("parameter installed complete trusted wrapper registration differs")
    native = definitions.read_strategy_spec(value.runtime.strategy.registration_fingerprint, as_of=now)
    if native != value.native_registration:
        raise PermissionError("parameter installed complete native registration differs")
    run = MinuteParameterFormalRunInput(start_date=value.runtime.start_date, end_date=value.runtime.end_date,
        parameters=MinuteParameterFormalParameters.from_prepared(value, prepared_publication))
    plan = _build_minute_formal_plan(registration=registration, identity=published.identity,
        parameters=_minute_formal_parameters(run), execution_profile=value.runtime.execution_profile,
        producer_commit=value.runtime.producer_commit, full_input_hash=value.full_input_hash, protocol=protocol,
        now=now, random_seed=random_seed, adapter_id="minute-parameter-replay", adapter_version="1",
        family_prefix="minute-parameter:", family_id=family_id, hypothesis_variant=hypothesis_variant)
    prepared = PreparedMinuteParameterRequest(frozen=value, published=published,
        prepared_publication=prepared_publication, catalog=catalog, registration=registration,
        formal_plan=plan, deadline=deadline, random_seed=random_seed)
    prepared.submission(job_id=UUID(int=0))
    return prepared


def register_minute_parameter_plan(value: FrozenMinuteParameterResearchInput,
    published: PublishedMinuteParameterInput, *, prepared_publication: MinuteParameterPreparedPublication,
    catalog: MinuteParameterReplayCatalog, definitions: ImmutableDefinitionRegistry, experiments: ExperimentRegistry,
    protocol: MinuteExperimentProtocol, now: datetime, deadline: datetime, random_seed: int = 0,
    family_id: str | None = None, hypothesis_variant: str = "minute-parameters-semantic2",
) -> PreparedMinuteParameterRequest:
    prepared = build_minute_parameter_plan(value, published, prepared_publication=prepared_publication,
        catalog=catalog, definitions=definitions, protocol=protocol, now=now, deadline=deadline,
        random_seed=random_seed, family_id=family_id, hypothesis_variant=hypothesis_variant)
    plan = prepared.formal_plan
    experiments.register_formal_plan(plan, family_manifest=HypothesisFamilyManifest(
        hypothesis_family=plan.spec.hypothesis_family, experiment_ids=(plan.spec.experiment_id,),
        search_space_fingerprint=canonical_sha256(prepared.run_input.parameters),
        metric_definition_fingerprint=plan.spec.metric_definition_fingerprint, preregistered_at=now))
    return prepared
