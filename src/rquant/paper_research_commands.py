"""Ownerless paper analysis intents and the original immutable Lab plan."""

from __future__ import annotations

from typing import Literal, Self
from collections.abc import Mapping
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from rquant.paper_portfolio_models import PaperPortfolioStateIdentity, Sha256
from rquant.paper_research import PaperResearchAdapterCatalog, NativePaperResearchAdapterCatalog, PaperResearchCatalog, PaperResearchRunParameters
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.research_run_spec import ResearchRunSpec
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel


class RunPaperPortfolioResearch(RuntimeContractModel):
    kind: Literal["run_paper_portfolio_research"] = "run_paper_portfolio_research"
    command_id: str
    requested_at: AwareUtcDatetime
    generation_id: Sha256
    account_id: str = Field(min_length=1, max_length=128)
    configuration_fingerprint: Sha256
    task_name: Literal["paper_reconcile", "paper_backtest_band"]
    backtest_job_id: UUID | None = None

    @field_validator("command_id")
    @classmethod
    def canonical_id(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("paper research requires the original canonical UUID")
        return value

    @model_validator(mode="after")
    def exact_task(self) -> Self:
        if (self.task_name == "paper_backtest_band") != (self.backtest_job_id is not None):
            raise ValueError("paper band requires its original sealed backtest job")
        return self


class OwnedRunPaperPortfolioResearch(RunPaperPortfolioResearch):
    owner_id: str
    metadata_identity: PaperPortfolioStateIdentity | StrategyAuthoringIdentity
    accepted_at: AwareUtcDatetime
    catalog: PaperResearchCatalog
    spec: ResearchRunSpec
    plan_hash: Sha256

    @model_validator(mode="before")
    @classmethod
    def original_source_identity(cls, value: object) -> object:
        if isinstance(value, Mapping):
            catalog = value.get("catalog")
            native = isinstance(catalog, NativePaperResearchAdapterCatalog) or (
                isinstance(catalog, Mapping) and catalog.get("contract") == "native-paper-research-catalog/v1")
            identity = value.get("metadata_identity")
            if identity is not None:
                raw = identity.model_dump(mode="python") if isinstance(identity, RuntimeContractModel) else identity
                kind = StrategyAuthoringIdentity if native else PaperPortfolioStateIdentity
                return {**value, "metadata_identity": kind.model_validate(raw)}
        return value

    def original(self) -> RunPaperPortfolioResearch:
        return RunPaperPortfolioResearch.model_validate(self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted_at", "catalog", "spec", "plan_hash"}))

    @model_validator(mode="after")
    def exact_plan(self) -> Self:
        from rquant.paper_research_adapter import paper_research_adapter_registry
        from rquant.runtime_contracts import canonical_sha256

        parameters = PaperResearchRunParameters.model_validate({item.name: item.value for item in self.spec.parameters.arguments})
        if isinstance(self.catalog, NativePaperResearchAdapterCatalog) and type(self.metadata_identity) is not StrategyAuthoringIdentity:
            raise ValueError("native owned analysis requires its original strategy owner metadata")
        if (self.spec.parameters.strategy_name, parameters.request_id, parameters.owner_id, parameters.account_id,
                parameters.configuration_fingerprint, self.catalog.metadata_identity) != (
                self.task_name, self.command_id, self.owner_id, self.account_id, self.configuration_fingerprint, self.metadata_identity):
            raise ValueError("paper owned run differs from its original account, request or metadata")
        plan = paper_research_adapter_registry(self.catalog).plan(self.spec)
        if canonical_sha256(plan) != self.plan_hash or not self.catalog.configuration.configured_at <= self.accepted_at < self.spec.deadline:
            raise ValueError("paper owned run differs from its frozen original plan or acceptance time")
        if len(self.model_dump_json().encode()) > 32*1024:
            raise ValueError("paper original plan exceeds its private command budget")
        return self


class PaperResearchSubmissionReceipt(RuntimeContractModel):
    command_id: str
    account_id: str
    configuration_fingerprint: Sha256
    task_name: Literal["paper_reconcile", "paper_backtest_band"]
    job_id: UUID
    lab_request_id: UUID
    lab_content_hash: Sha256
    spec_hash: Sha256
    accepted_at: AwareUtcDatetime
    submitted_at: AwareUtcDatetime

    @model_validator(mode="after")
    def original_job(self) -> Self:
        if str(self.job_id) != self.command_id or self.submitted_at < self.accepted_at:
            raise ValueError("paper submission does not reference its original accepted job")
        return self
