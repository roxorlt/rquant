"""Small writer-owned commands and original-journal admission markers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import Field, JsonValue

from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import SubmitJobCommand
from rquant.portfolio_backtest_artifact import PortfolioZipExportFacade
from rquant.portfolio_backtest_models import PortfolioBacktestConfig
from rquant.portfolio_backtest_source import PreparedPortfolioRequest
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256


class SubmitPortfolioBacktest(RuntimeContractModel):
    kind: Literal["submit_portfolio_backtest"] = "submit_portfolio_backtest"
    command_id: str = Field(pattern=r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
    requested_at: AwareUtcDatetime
    actor_id: str = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    config: PortfolioBacktestConfig


class ExportPortfolioBacktestZip(RuntimeContractModel):
    kind: Literal["export_portfolio_backtest_zip"] = "export_portfolio_backtest_zip"
    command_id: str = Field(pattern=r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
    requested_at: AwareUtcDatetime
    actor_id: str = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    job_id: UUID
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


PortfolioCommand = Annotated[
    SubmitPortfolioBacktest | ExportPortfolioBacktestZip, Field(discriminator="kind")
]


class PortfolioRunEffect(RuntimeContractModel):
    contract: Literal["portfolio-admission/v1"] = "portfolio-admission/v1"
    kind: Literal["submit"] = "submit"
    command_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    interaction_key: str
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    command: SubmitJobCommand


class PortfolioZipEffect(RuntimeContractModel):
    contract: Literal["portfolio-admission/v1"] = "portfolio-admission/v1"
    kind: Literal["zip"] = "zip"
    command_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_id: UUID
    job_id: UUID
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class PortfolioPageControlBackend(Protocol):
    def freeze(self, command: PortfolioCommand) -> JsonValue: ...

    def submit(self, command: PortfolioCommand, marker: JsonValue) -> JsonValue: ...

    def recover(self, command: PortfolioCommand, marker: JsonValue) -> JsonValue | None: ...


def portfolio_interaction(command: SubmitPortfolioBacktest) -> str:
    return f"web.portfolio:{command.actor_id}:{command.command_id}"


def portfolio_job_id(actor_id: str, command_id: UUID | str) -> UUID:
    return uuid5(NAMESPACE_URL, f"rquant.portfolio-job:{actor_id}:{command_id}")


class PortfolioCommandWriter:
    def __init__(
        self,
        *,
        commands: LabCommandSubmissionFacade,
        prepare: Callable[[PortfolioBacktestConfig], PreparedPortfolioRequest],
        exports: PortfolioZipExportFacade | None = None,
    ) -> None:
        self.commands, self.prepare, self.exports = commands, prepare, exports

    def freeze(self, command: PortfolioCommand) -> JsonValue:
        if isinstance(command, SubmitPortfolioBacktest):
            value = self.prepare(command.config)
            checked = PreparedPortfolioRequest.model_validate(value.model_dump(mode="python"))
            if checked.frozen.config != command.config:
                raise PermissionError("portfolio producer returned another config")
            submission = checked.submission(
                job_id=portfolio_job_id(command.actor_id, command.command_id)
            )
            marker = PortfolioRunEffect(
                command_hash=canonical_sha256(command),
                config_hash=command.config.config_hash,
                interaction_key=portfolio_interaction(command),
                command=submission.command,
            )
        else:
            if self.exports is None:
                raise RuntimeError("portfolio export is unavailable")
            self.exports.result_reader.read(
                command.job_id, expected_result_hash=command.result_hash
            ).html_bytes()
            marker = PortfolioZipEffect(
                command_hash=canonical_sha256(command),
                request_id=uuid5(
                    NAMESPACE_URL, f"rquant.portfolio-zip:{command.actor_id}:{command.command_id}"
                ),
                job_id=command.job_id,
                result_hash=command.result_hash,
            )
        return marker.model_dump(mode="json")

    def _marker(
        self, command: PortfolioCommand, marker: JsonValue
    ) -> PortfolioRunEffect | PortfolioZipEffect:
        model = (
            PortfolioRunEffect
            if isinstance(command, SubmitPortfolioBacktest)
            else PortfolioZipEffect
        )
        checked = model.model_validate(marker)
        if checked.command_hash != canonical_sha256(command):
            raise PermissionError("portfolio original command differs")
        if isinstance(checked, PortfolioRunEffect):
            arguments = {p.name: p.value for p in checked.command.spec.parameters.arguments}
            if (
                checked.config_hash != command.config.config_hash
                or arguments.get("config_hash") != checked.config_hash
                or (
                    checked.command.job_id,
                    checked.interaction_key,
                    checked.command.spec.schema_version,
                    checked.command.spec.parameters.strategy_name,
                )
                != (
                    portfolio_job_id(command.actor_id, command.command_id),
                    portfolio_interaction(command),
                    3,
                    "portfolio_backtest",
                )
            ):
                raise PermissionError("portfolio frozen task differs from original config")
        elif (checked.job_id, checked.result_hash) != (command.job_id, command.result_hash):
            raise PermissionError("portfolio ZIP original result differs")
        return checked

    def submit(self, command: PortfolioCommand, marker: JsonValue) -> JsonValue:
        checked = self._marker(command, marker)
        if isinstance(checked, PortfolioRunEffect):
            return self.commands.submit_create(
                checked.command, interaction_key=checked.interaction_key
            ).model_dump(mode="json")
        if self.exports is None:
            raise RuntimeError("portfolio export is unavailable")
        return self.exports.export_portfolio(
            checked.job_id, request_id=checked.request_id, expected_result_hash=checked.result_hash
        ).model_dump(mode="json")

    def recover(self, command: PortfolioCommand, marker: JsonValue) -> JsonValue | None:
        checked = self._marker(command, marker)
        if isinstance(checked, PortfolioRunEffect):
            # Original Lab submit_create restores exactly this envelope/attempt.
            return self.commands.submit_create(
                checked.command, interaction_key=checked.interaction_key
            ).model_dump(mode="json")
        if self.exports is None:
            raise RuntimeError("portfolio export is unavailable")
        receipt = self.exports.recover_portfolio(
            checked.job_id, request_id=checked.request_id, expected_result_hash=checked.result_hash
        )
        return None if receipt is None else receipt.model_dump(mode="json")
