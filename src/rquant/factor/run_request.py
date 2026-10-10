"""Public run parameters contain no source paths, identity claims or formula text."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.registry import FactorHeadRef
from rquant.factor.universe import UniverseSelection
from rquant.runtime_contracts import canonical_sha256

RUN_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
NeutralizationMode = Literal["none", "industry", "industry_size"]
NeutralizationLabel = Literal["无", "行业", "行业 + 市值"]


def neutralization_label(mode: NeutralizationMode) -> NeutralizationLabel:
    return {"none": "无", "industry": "行业", "industry_size": "行业 + 市值"}[mode]


class FactorRunParameters(BaseModel):
    model_config = RUN_IMMUTABLE

    factor_id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    expected_head: FactorHeadRef
    selection: UniverseSelection
    start_date: date
    end_date: date
    holding_sessions: Literal[1, 5, 10, 20]
    group_count: Literal[3, 5, 10] = 5
    ic_method: Literal["rank", "normal"] = "rank"
    neutralization: NeutralizationMode = "none"
    mad_multiple: float | None = Field(
        default=None, strict=True, gt=0, allow_inf_nan=False, exclude_if=lambda v: v is None
    )
    extended_statistics: bool = Field(default=False, exclude_if=lambda v: v is False)

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def _date(cls, value: object) -> date:
        if type(value) is str:
            parsed = date.fromisoformat(value)
            if parsed.isoformat() != value:
                raise ValueError("日期格式不正确")
            return parsed
        if type(value) is not date:
            raise ValueError("日期格式不正确")
        return value

    @field_validator("holding_sessions", "group_count", mode="before")
    @classmethod
    def _integer(cls, value: object) -> int:
        if type(value) is not int:
            raise ValueError("周期与分组必须为整数")
        return value

    @model_validator(mode="after")
    def _range(self) -> FactorRunParameters:
        if self.start_date > self.end_date:
            raise ValueError("起止日期顺序不正确")
        return self


class FactorRunRequest(BaseModel):
    model_config = RUN_IMMUTABLE

    command_id: str
    requested_at: AwareDatetime
    serving_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    parameters: FactorRunParameters

    @field_validator("command_id")
    @classmethod
    def _uuid(cls, value: str) -> str:
        if str(UUID(value)) != value:
            raise ValueError("运行编号必须为规范 UUID")
        return value

    @field_validator("requested_at", mode="before")
    @classmethod
    def _time(cls, value: object) -> datetime:
        if type(value) is str:
            return datetime.fromisoformat(value)
        if not isinstance(value, datetime):
            raise ValueError("请求时刻格式不正确")
        return value

    @property
    def request_sha256(self) -> str:
        return canonical_sha256(self)


class FactorRunPoolOption(BaseModel):
    model_config = RUN_IMMUTABLE

    selection: UniverseSelection
    label: str = Field(min_length=1, max_length=40)
    available: bool
    reason: str | None = Field(default=None, max_length=80)


class FactorRunNeutralizationOption(BaseModel):
    model_config = RUN_IMMUTABLE

    neutralization: NeutralizationMode
    label: str = Field(min_length=1, max_length=40)
    available: bool
    reason: str | None = Field(default=None, max_length=80)


class FactorRunAvailability(BaseModel):
    model_config = RUN_IMMUTABLE

    enabled: bool
    reason: str | None = Field(default=None, max_length=80)
    pools: tuple[FactorRunPoolOption, ...]
    start_date: date | None = None
    end_date: date | None = None
    neutralizations: tuple[FactorRunNeutralizationOption, ...] | None = Field(
        default=None, max_length=3, exclude_if=lambda value: value is None
    )


class FactorRunOperationResult(BaseModel):
    model_config = RUN_IMMUTABLE

    original_request: FactorRunRequest
    status: Literal["pending", "processing", "submitted", "uncertain", "rejected"]
    reason: str | None = Field(default=None, max_length=80)
    job_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    spec_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _submitted(self) -> FactorRunOperationResult:
        if self.status == "submitted" and (self.job_id is None or self.spec_sha256 is None):
            raise ValueError("提交回执缺少原任务")
        if (self.job_id is None) != (self.spec_sha256 is None):
            raise ValueError("任务绑定不完整")
        return self
