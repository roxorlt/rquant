"""Operator requests, confirmed publication bodies and actual admission facts."""

from typing import TYPE_CHECKING, Literal, Self
from uuid import UUID

from pydantic import Field, StrictBool, field_validator, model_validator

from rquant.paper_portfolio_models import PaperPortfolioBinding, PaperPortfolioConfiguration, PaperPortfolioRules, PaperPortfolioStateIdentity, Sha256
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256


class SetPaperAccountPaused(RuntimeContractModel):
    kind: Literal["set_paper_account_paused"] = "set_paper_account_paused"
    command_id: str
    requested_at: AwareUtcDatetime
    generation_id: str = Field(min_length=1, max_length=128)
    account_id: str = Field(min_length=1, max_length=128)
    configuration_fingerprint: Sha256
    expected_sequence: int = Field(strict=True, ge=0, le=2**31 - 2)
    expected_paused: StrictBool
    paused: StrictBool

    @field_validator("command_id")
    @classmethod
    def canonical_id(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("operator command requires the original canonical UUID")
        return value

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperOperatorControl(RuntimeContractModel):
    schema_version: Literal[2] = 2
    contract: Literal["paper-operator-control/v2"] = "paper-operator-control/v2"
    binding: PaperPortfolioBinding
    configuration_fingerprint: Sha256
    configuration_version: int = Field(strict=True, ge=1)
    state_instance_id: str
    sequence: int = Field(strict=True, ge=1, le=2**31 - 1)
    original_command_id: str
    original_request_fingerprint: Sha256
    issued_at: AwareUtcDatetime
    paused: StrictBool

    @model_validator(mode="after")
    def bounded(self) -> Self:
        UUID(self.state_instance_id)
        SetPaperAccountPaused.canonical_id(self.original_command_id)
        if len(self.model_dump_json().encode()) > 16384:
            raise ValueError("operator body exceeds its fixed byte budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperOperatorApplication(RuntimeContractModel):
    account_id: str
    configuration_fingerprint: Sha256
    sequence: int = Field(strict=True, ge=0, le=2**31 - 1)
    control_fingerprint: Sha256 | None = None
    paused: StrictBool = True
    status: Literal["waiting", "applied", "unavailable"]
    observed_at: AwareUtcDatetime | None = None
    reason: str | None = Field(default=None, max_length=512)

    @model_validator(mode="after")
    def closed_when_unavailable(self) -> Self:
        if self.status != "applied" and not self.paused:
            raise ValueError("unapplied operator control cannot open admission")
        if self.status == "applied" and (self.sequence == 0 or self.control_fingerprint is None or self.observed_at is None):
            raise ValueError("applied control requires its actual sequence and time")
        return self


class SavePaperPortfolioConfiguration(PaperPortfolioRules):
    kind: Literal["save_paper_portfolio_configuration"] = "save_paper_portfolio_configuration"
    command_id: str
    requested_at: AwareUtcDatetime
    generation_id: str = Field(min_length=1, max_length=128)
    account_id: str = Field(min_length=1, max_length=128)
    expected_configuration_fingerprint: Sha256

    _canonical_id = field_validator("command_id")(SetPaperAccountPaused.canonical_id.__func__)

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class PaperOperatorConfirmation(RuntimeContractModel):
    confirmation_id: str
    owner_id: str
    metadata_identity: PaperPortfolioStateIdentity
    request: SetPaperAccountPaused
    issued_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class OwnedSetPaperAccountPaused(SetPaperAccountPaused):
    owner_id: str
    metadata_identity: PaperPortfolioStateIdentity
    accepted_at: AwareUtcDatetime
    confirmation: PaperOperatorConfirmation

    def original(self) -> SetPaperAccountPaused:
        return SetPaperAccountPaused.model_validate(self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted_at", "confirmation"}))

    @model_validator(mode="after")
    def confirmed_request(self) -> Self:
        if (self.confirmation.request != self.original() or self.confirmation.owner_id != self.owner_id
                or self.confirmation.metadata_identity != self.metadata_identity
                or not self.confirmation.issued_at <= self.accepted_at < self.confirmation.expires_at):
            raise ValueError("paper owned pause differs from the exact confirmed request")
        return self


class OwnedSavePaperPortfolioConfiguration(SavePaperPortfolioConfiguration):
    owner_id: str
    metadata_identity: PaperPortfolioStateIdentity
    accepted_at: AwareUtcDatetime
    configuration: PaperPortfolioConfiguration

    def original(self) -> SavePaperPortfolioConfiguration:
        return SavePaperPortfolioConfiguration.model_validate(self.model_dump(mode="python", exclude={"owner_id", "metadata_identity", "accepted_at", "configuration"}))

    @model_validator(mode="after")
    def frozen_rules(self) -> Self:
        if (self.configuration.binding.owner_id != self.owner_id or self.configuration.binding.account_id != self.account_id
                or self.configuration.weight_rule != self.weight_rule or self.configuration.drawdown_rule != self.drawdown_rule
                or self.configuration.configured_at != self.accepted_at):
            raise ValueError("paper owned configuration differs from the accepted rules")
        return self


if TYPE_CHECKING:
    from rquant.paper_research_commands import OwnedRunPaperPortfolioResearch, RunPaperPortfolioResearch

    PaperPortfolioCommand = SetPaperAccountPaused | SavePaperPortfolioConfiguration | RunPaperPortfolioResearch
    OwnedPaperPortfolioCommand = OwnedSetPaperAccountPaused | OwnedSavePaperPortfolioConfiguration | OwnedRunPaperPortfolioResearch


def __getattr__(name: str) -> object:
    # The minute profile reads operator types before research types exist.
    if name not in {"OwnedRunPaperPortfolioResearch", "RunPaperPortfolioResearch",
                    "PaperPortfolioCommand", "OwnedPaperPortfolioCommand"}:
        raise AttributeError(name)
    from rquant.paper_research_commands import OwnedRunPaperPortfolioResearch, RunPaperPortfolioResearch

    return {
        "RunPaperPortfolioResearch": RunPaperPortfolioResearch,
        "OwnedRunPaperPortfolioResearch": OwnedRunPaperPortfolioResearch,
        "PaperPortfolioCommand": SetPaperAccountPaused | SavePaperPortfolioConfiguration | RunPaperPortfolioResearch,
        "OwnedPaperPortfolioCommand": OwnedSetPaperAccountPaused | OwnedSavePaperPortfolioConfiguration | OwnedRunPaperPortfolioResearch,
    }[name]
