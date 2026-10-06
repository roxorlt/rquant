"""Ownerless requests and server facts for durable screening commands."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import Field, field_validator, model_validator

from rquant.llm.registry import get_rule_spec
from rquant.llm.schemas import RuleCall
from rquant.manual_watchlist import OwnerId
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.web.models.screen import ScreenRankingPlan, ScreenRow, ScreenSourceInfo, ScreenStep


class ScreenQueryDefinition(RuntimeContractModel):
    schema_version: Literal[1] = 1
    description: str = Field(default="", max_length=500)
    mode: Literal["daily", "intraday"] = "daily"
    trade_date: date
    source_kind: Literal["serving", "replica", "intraday"]
    source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    cutoff: AwareUtcDatetime | None = None
    conditions: tuple[RuleCall, ...] = Field(min_length=1, max_length=26)
    ranking: ScreenRankingPlan | None = None

    @field_validator("conditions", mode="before")
    @classmethod
    def reject_untyped_rule_context(cls, value: object) -> object:
        if isinstance(value, (list, tuple)):
            for call in value:
                if isinstance(call, dict) and set(call) - {"name", "args"}:
                    raise ValueError("screen conditions cannot contain extra context")
        return value

    @property
    def normalized_plan_sha256(self) -> str:
        return canonical_sha256(
            {
                "mode": self.mode,
                "trade_date": self.trade_date,
                "conditions": self.conditions,
                "ranking": self.ranking,
            }
        )

    @model_validator(mode="after")
    def validate_definition(self) -> ScreenQueryDefinition:
        if (self.mode == "intraday") != (self.source_kind == "intraday"):
            raise ValueError("screen mode and source must agree")
        if self.mode == "intraday" and self.cutoff is None:
            raise ValueError("intraday screening requires an actual cutoff")
        normalized = []
        for call in self.conditions:
            spec = get_rule_spec(call.name)
            if set(call.args) - set(spec.args_model.model_fields):
                raise ValueError("screen condition arguments contain unknown fields")
            normalized.append(
                RuleCall(
                    name=call.name,
                    args=spec.args_model.model_validate(call.args).model_dump(mode="json"),
                )
            )
        object.__setattr__(self, "conditions", tuple(normalized))
        canonical_sha256(self)
        return self


class ExecuteScreenQuery(RuntimeContractModel):
    kind: Literal["execute_screen_query"] = "execute_screen_query"
    command_id: str = Field(min_length=1, max_length=128)
    requested_at: AwareUtcDatetime
    definition: ScreenQueryDefinition
    page_size: int = Field(default=20, ge=1, le=100)


class _OwnedExecuteScreenQuery(ExecuteScreenQuery):
    owner_id: OwnerId


class ScreenQueryExecution(RuntimeContractModel):
    owner_id: OwnerId = Field(exclude=True)
    execution_id: str
    sequence: int = Field(ge=1)
    command_hash: str
    plan_hash: str
    definition: ScreenQueryDefinition
    original_command: ExecuteScreenQuery
    source: ScreenSourceInfo | None = None
    started_at: AwareUtcDatetime | None = None
    completed_at: AwareUtcDatetime | None = None
    status: Literal["pending", "processing", "succeeded", "failed", "source_expired", "unknown"]
    base_count: int | None = None
    total: int | None = None
    unknown_count: int | None = None
    ranked_count: int | None = None
    steps: tuple[ScreenStep, ...] = ()
    artifact_sha256: str | None = None
    member_rank_sha256: str | None = None
    failure_code: str | None = None


class ScreenHistoryPage(RuntimeContractModel):
    owner_scope_tag: str
    items: tuple[ScreenQueryExecution, ...]
    next_cursor: str | None


class ScreenExecutionResults(RuntimeContractModel):
    execution_id: str
    artifact_sha256: str
    rows: tuple[ScreenRow, ...]
    next_cursor: str | None


class ScreenPresetDefinition(RuntimeContractModel):
    preset_id: str = Field(pattern=r"^[A-Za-z0-9_-]{1,80}$")
    name: str = Field(min_length=1, max_length=80)
    definition: ScreenQueryDefinition


class ScreenQueryPreset(ScreenPresetDefinition):
    version: int = Field(ge=1)
    updated_at: AwareUtcDatetime
    command_hash: str
