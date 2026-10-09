"""Exact metadata and source identities for two private exploratory analyses."""

from __future__ import annotations

from datetime import date
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from rquant.paper_portfolio_models import PaperPortfolioConfiguration, PaperPortfolioStateIdentity, Sha256
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.paper_portfolio_band import PaperResearchBandInput, NativePaperBacktestBandInput
from rquant.paper_reconcile import PaperReconcileInput
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_promotion_contracts import NativeMinuteForwardConfiguration

PAPER_RESEARCH_TASKS = frozenset({"paper_reconcile", "paper_backtest_band"})
PAPER_RESEARCH_INPUT_TABLE = "paper_research_input"
PAPER_RESEARCH_INPUT_CONTRACT = "paper-research-input/v1"
MAX_PAPER_RESEARCH_INPUT_BYTES = 16*1024*1024


class PaperResearchAdapterCatalog(RuntimeContractModel):
    contract: Literal["paper-research-catalog/v1"] = "paper-research-catalog/v1"
    metadata_identity: PaperPortfolioStateIdentity
    configuration: PaperPortfolioConfiguration
    source_code_identity: Sha256

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class NativePaperResearchAdapterCatalog(RuntimeContractModel):
    contract: Literal["native-paper-research-catalog/v1"] = "native-paper-research-catalog/v1"
    metadata_identity: StrategyAuthoringIdentity
    configuration: NativeMinuteForwardConfiguration
    source_code_identity: Sha256

    @model_validator(mode="after")
    def original_strategy_owner(self) -> Self:
        if self.metadata_identity != self.configuration.metadata_identity:
            raise ValueError("native catalog differs from its original strategy metadata owner")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


PaperResearchCatalog = PaperResearchAdapterCatalog | NativePaperResearchAdapterCatalog


class PaperResearchRunParameters(RuntimeContractModel):
    owner_id: str = Field(min_length=1, max_length=128)
    account_id: str = Field(min_length=1, max_length=128)
    configuration_fingerprint: Sha256
    configuration_version: int = Field(strict=True, ge=1, le=4096)
    strategy_id: str = Field(min_length=1, max_length=128)
    strategy_version: str = Field(min_length=1, max_length=64)
    parameter_fingerprint: Sha256
    cost_spec_id: Sha256
    source_code_identity: Sha256
    input_hash: Sha256
    request_id: str
    work_units: int = Field(strict=True, ge=1, le=100000)

    @field_validator("request_id")
    @classmethod
    def canonical_request(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("paper research requires the original canonical request UUID")
        return value

    def require_catalog(self, catalog: PaperResearchCatalog) -> None:
        config, binding = catalog.configuration, catalog.configuration.binding
        if (self.owner_id, self.account_id, self.configuration_fingerprint, self.configuration_version,
                self.strategy_id, self.strategy_version, self.parameter_fingerprint, self.cost_spec_id, self.source_code_identity) != (
                binding.owner_id, binding.account_id, config.fingerprint, config.version, binding.strategy_id, binding.strategy_version,
                binding.parameter_fingerprint, binding.cost_spec_id, catalog.source_code_identity):
            raise ValueError("paper research differs from its exact account, owner, configuration, strategy or source")

    @classmethod
    def from_input(cls, value: FrozenPaperResearchInput, *, request_id: str) -> Self:
        config, binding = value.catalog.configuration, value.catalog.configuration.binding
        units = (len(value.band.comparison_dates) if value.band is not None else
                 1+len(value.reconcile.expected.history)+sum(len(item.fills) for item in value.reconcile.expected.history))
        return cls(owner_id=binding.owner_id, account_id=binding.account_id, configuration_fingerprint=config.fingerprint,
                   configuration_version=config.version, strategy_id=binding.strategy_id, strategy_version=binding.strategy_version,
                   parameter_fingerprint=binding.parameter_fingerprint, cost_spec_id=binding.cost_spec_id,
                   source_code_identity=value.catalog.source_code_identity, input_hash=value.fingerprint,
                   request_id=request_id, work_units=units)


class FrozenPaperResearchInput(RuntimeContractModel):
    contract: Literal["frozen-paper-research/v1"] = "frozen-paper-research/v1"
    task_name: Literal["paper_reconcile", "paper_backtest_band"]
    catalog: PaperResearchCatalog
    code_sha: str = Field(pattern=r"^[0-9a-f]{40}$")
    available_at: AwareUtcDatetime
    reconcile: PaperReconcileInput | None = None
    band: PaperResearchBandInput | None = None

    @model_validator(mode="after")
    def complete_exact_input(self) -> Self:
        if ((self.task_name == "paper_reconcile") != (self.reconcile is not None)
                or (self.task_name == "paper_backtest_band") != (self.band is not None)):
            raise ValueError("paper research requires one exact complete input")
        source = self.reconcile if self.reconcile is not None else self.band
        if isinstance(self.catalog, NativePaperResearchAdapterCatalog) and not isinstance(self.band, NativePaperBacktestBandInput):
            raise ValueError("native research requires its native band and original complete ledger reconciliation")
        if source.configuration != self.catalog.configuration or self.catalog.configuration.configured_at > self.available_at:
            raise ValueError("paper research input differs from its immutable configured source")
        if self.reconcile is not None and self.reconcile.as_of > self.available_at:
            raise ValueError("paper reconciliation contains a future ledger cutoff")
        if self.band is not None and (self.band.comparison_dates[-1] > self.available_at.date()
                                     or any(item.trade_date > self.available_at.date() for item in self.band.backtest.returns)):
            raise ValueError("paper interval contains future comparison or return dates")
        if len(self.model_dump_json().encode()) > MAX_PAPER_RESEARCH_INPUT_BYTES:
            raise ValueError("paper research input exceeds the original immutable source budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json" if isinstance(self.catalog, NativePaperResearchAdapterCatalog) else "python"))

    @property
    def dates(self) -> tuple[date, date]:
        if self.band is not None:
            return self.band.comparison_dates[0], self.band.comparison_dates[-1]
        return self.reconcile.as_of.date(), self.reconcile.as_of.date()
