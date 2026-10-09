"""Exact original template identities for private formal experiments."""

from __future__ import annotations

from datetime import date
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from rquant.backtest.contracts import Sha256
from rquant.definition_registry import StrategySpecRegistration
from rquant.experiment_registry import FormalExperimentPlan
from rquant.lab_job_center import ResearchJobSubmission
from rquant.portfolio_backtest_models import FrozenPortfolioInput, PortfolioBacktestConfig
from rquant.research_run_spec import ResearchRunSpec
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strategy_authoring_commands import (
    ExperimentTemplateSelection,
    SaveStrategyTemplate,
    StrategyAuthoringIdentity,
    StrategyTemplateHead,
    StrategyTemplateReceipt,
)
from rquant.strategy_template_adapter import (
    StrategyTemplateAdapter,
    StrategyTemplateAdapterCatalog,
    StrategyTemplateRunParameters,
)
from rquant.strategy_template_definition import StrategyTemplateExecutionVersion
from rquant.strategy_template_run import FrozenStrategyTemplateInput
from rquant.strategy_template_source import PublishedStrategyTemplateInput


class ExperimentTemplateBaseline(RuntimeContractModel):
    metadata_identity: StrategyAuthoringIdentity
    version: StrategyTemplateExecutionVersion
    name: str = Field(min_length=1, max_length=80)
    catalog_hash: Sha256
    generation_id: str


class ExperimentTemplateSlot(RuntimeContractModel):
    owner: str
    family_id: str
    index: int = Field(strict=True, ge=0, le=63)
    baseline_hash: Sha256
    request: SaveStrategyTemplate
    state: Literal["pending", "saved", "failed", "cancelled"] = "pending"
    receipt: StrategyTemplateReceipt | None = None
    failure: Literal["capacity", "source_changed", "invalid_definition"] | None = None


class ExperimentTemplatePublication(PublishedStrategyTemplateInput):
    config_hash: Sha256


class PreparedExperimentTemplate(RuntimeContractModel):
    configuration: PortfolioBacktestConfig
    frozen: FrozenStrategyTemplateInput
    published: ExperimentTemplatePublication
    registration: StrategySpecRegistration
    formal_plan: FormalExperimentPlan
    spec: ResearchRunSpec
    catalog: StrategyTemplateAdapterCatalog
    benchmark_closes: tuple[tuple[date, float], ...] | None = None

    @model_validator(mode="after")
    def exact_original_bindings(self) -> Self:
        if len(self.catalog.versions) != 1:
            raise ValueError("private template child requires its single exact catalog version")
        version = self.catalog.versions[0]
        if (
            version.definition != self.registration
            or self.registration != self.frozen.definition
            or version.rules != self.frozen.rules
            or version.owner_id != self.frozen.owner_id
            or self.published.input_hash != self.frozen.input_hash
            or self.published.config_hash != self.configuration.config_hash
            or self.spec.dataset_snapshot != self.published.identity
            or self.spec.experiment is None
            or self.spec.experiment.spec != self.formal_plan.spec
            or self.spec.code_sha != self.frozen.request.producer_commit
            or self.spec.execution_costs != self.frozen.request.execution_cost_spec
        ):
            raise ValueError(
                "private template preparation differs from its original definition or source"
            )
        parameters = StrategyTemplateAdapter(version.strategy_id, catalog=self.catalog).parameters(
            self.spec
        )
        if parameters != StrategyTemplateRunParameters.from_input(
            self.frozen, request_id=parameters.request_id
        ):
            raise ValueError(
                "private template run parameters differ from its complete original input"
            )
        # This invokes only the original config/benchmark validator, never the C6 executor.
        FrozenPortfolioInput(
            config=self.configuration,
            request=self.frozen.request,
            sources=self.frozen.sources,
            benchmark_closes=self.benchmark_closes,
            benchmark_unavailable="missing_source" if self.benchmark_closes is None else None,
        )
        return self

    def submission(self, *, job_id: UUID) -> ResearchJobSubmission:
        from rquant.lab_job_center import _preflight_research_plan
        from rquant.lab_job_protocol import SubmitJobCommand

        _preflight_research_plan(self.spec, template_catalog=self.catalog)
        return ResearchJobSubmission(
            spec=self.spec, command=SubmitJobCommand(job_id=job_id, spec=self.spec, max_attempts=2)
        )
