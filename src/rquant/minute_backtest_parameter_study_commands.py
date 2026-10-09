"""Pure study requests for the original PageControl command and execution owner."""

from __future__ import annotations

from typing import Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from rquant.minute_backtest_contracts import MAX_DATE_SPAN, MAX_INPUT_BYTES, MAX_WORK_UNITS, Sha256
from rquant.minute_backtest_formal import MinuteExperimentProtocol
from rquant.minute_backtest_parameter_search import MinuteParameterSearchRequest
from rquant.minute_backtest_parameter_study import MinuteParameterStudySettings
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.minute_backtest_publication_contracts import MAX_MINUTE_CONTROL_BYTES
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256


class MinuteParameterStudyWindowSettings(RuntimeContractModel):
    fold_count: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)
    min_training_dates: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)
    validation_date_count: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)


class MinuteParameterStudyExecutionRequest(RuntimeContractModel):
    """A complete request; the installed prepare owner supplies trusted heads."""

    request_id: UUID
    owner_id: str = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(strict=True, ge=1)
    full_input_hash: Sha256
    parameters: MinuteParameterSet
    formal_protocol: MinuteExperimentProtocol
    settings: tuple[MinuteParameterStudySettings, ...] = Field(
        min_length=1, max_length=MAX_WORK_UNITS
    )
    random_seed: int = Field(strict=True, ge=0, lt=2**63)
    requested_at: AwareUtcDatetime
    deadline: AwareUtcDatetime
    mode: Literal["single", "grid", "random", "ablation", "walk_forward"]
    search: MinuteParameterSearchRequest | None = None
    walk_forward: MinuteParameterStudyWindowSettings | None = None

    @model_validator(mode="after")
    def complete_controls(self) -> Self:
        if self.deadline <= self.requested_at:
            raise ValueError("study deadline must follow the original request")
        if len({canonical_sha256(item) for item in self.settings}) != len(self.settings):
            raise ValueError("duplicate study selection controls")
        if (self.mode == "walk_forward") != (self.walk_forward is not None):
            raise ValueError("walk-forward requires explicit three-part window controls")
        if self.mode in {"grid", "random"}:
            if self.search is None or self.search.mode != self.mode:
                raise ValueError("study search mode requires its complete original search request")
        elif self.mode != "walk_forward" and self.search is not None:
            raise ValueError("this study mode cannot replace its recipe owner with a search")
        if self.search is not None and (
            self.search.base != self.parameters or self.search.seed != self.random_seed
        ):
            raise ValueError("search differs from the complete study recipe or seed")
        _study_control_size(self)
        return self


def _study_control_size(value: RuntimeContractModel) -> None:
    if len(value.model_dump_json(exclude_computed_fields=True).encode("utf-8")) > min(
        MAX_INPUT_BYTES, MAX_MINUTE_CONTROL_BYTES
    ):
        raise ValueError("study exceeds the original minute input/control byte budget")


class SubmitMinuteParameterStudy(RuntimeContractModel):
    """The original parent UUID/body; trusted actor and persistence remain with PageControl."""

    kind: Literal["submit_minute_parameter_study"] = "submit_minute_parameter_study"
    command_id: str = Field(pattern=r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
    requested_at: AwareUtcDatetime
    actor_id: str = Field(pattern=r"^[A-Za-z0-9._@-]{1,64}$")
    request: MinuteParameterStudyExecutionRequest

    @model_validator(mode="after")
    def complete_original_request(self) -> Self:
        if (self.command_id, self.actor_id, self.requested_at) != (
            str(self.request.request_id),
            self.request.owner_id,
            self.request.requested_at,
        ):
            raise ValueError("study command differs from its complete original request")
        _study_control_size(self)
        return self
