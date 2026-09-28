"""Durable ranking definition for daily user pools."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import Field, StrictBool, model_validator

from rquant.runtime_contracts import RuntimeContractModel

PoolRankingMetric = Literal[
    "RETURN_20D_PCT[0]", "TURNOVER_RATE[0]", "CIRC_MV[0]", "PCT_CHG[0]"
]


class PoolRankingCondition(RuntimeContractModel):
    metric: PoolRankingMetric
    ascending: StrictBool
    weight: float = Field(ge=0, le=100, allow_inf_nan=False)


class PoolRankingPlan(RuntimeContractModel):
    conditions: tuple[PoolRankingCondition, ...] = Field(min_length=1, max_length=4)
    top_n: int = Field(ge=1, le=100, strict=True)

    @model_validator(mode="after")
    def validate_weights_and_metrics(self) -> Self:
        if sum(condition.weight for condition in self.conditions) <= 0:
            raise ValueError("ranking needs positive total weight")
        metrics = [condition.metric for condition in self.conditions]
        if len(set(metrics)) != len(metrics):
            raise ValueError("ranking metrics must be distinct")
        return self
