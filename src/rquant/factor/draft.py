"""Bounded browser input for a server-built historical daily factor definition."""

from __future__ import annotations

import hashlib
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.factor.capability import HISTORICAL_DAILY_V1
from rquant.factor.definition import FactorDefinition, build_factor_definition
from rquant.factor.evaluate import FactorDirection
from rquant.factor.registry import FactorHeadRef
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.strict_json import canonical_json_bytes


class FactorSaveDraft(BaseModel):
    """The complete original browser request; no derived definition fields are accepted."""

    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")

    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    requested_at: AwareUtcDatetime
    mode: Literal["create", "edit"]
    factor_id: str | None = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    expected_head: FactorHeadRef | None
    name_zh: str = Field(min_length=1, max_length=80)
    category: str = Field(min_length=1, max_length=64)
    direction: FactorDirection
    expression: str = Field(min_length=1, max_length=2048)

    @model_validator(mode="after")
    def _check_target(self) -> FactorSaveDraft:
        if self.mode == "create" and (self.factor_id is not None or self.expected_head is not None):
            raise ValueError("new factor cannot supply an existing head")
        if self.mode == "edit" and (self.factor_id is None or self.expected_head is None):
            raise ValueError("edited factor requires its exact head")
        return self


def draft_sha256(draft: FactorSaveDraft) -> str:
    checked = FactorSaveDraft.model_validate(draft)
    return hashlib.sha256(canonical_json_bytes(checked.model_dump(mode="json"))).hexdigest()


def draft_factor_id(draft: FactorSaveDraft, *, authenticated_actor_id: str) -> str:
    checked = FactorSaveDraft.model_validate(draft)
    if not authenticated_actor_id:
        raise ValueError("authenticated actor is required")
    if checked.factor_id is not None:
        return checked.factor_id
    key = canonical_json_bytes(
        {"actor_id": authenticated_actor_id, "command_id": checked.command_id}
    )
    return f"f_{hashlib.sha256(key).hexdigest()[:40]}"


def build_draft_definition(
    draft: FactorSaveDraft, *, authenticated_actor_id: str
) -> FactorDefinition:
    checked = FactorSaveDraft.model_validate(draft)
    factor_id = draft_factor_id(checked, authenticated_actor_id=authenticated_actor_id)
    definition = build_factor_definition(
        factor_id=factor_id,
        name_zh=checked.name_zh,
        category=checked.category,
        direction=checked.direction,
        version=1 if checked.expected_head is None else checked.expected_head.version + 1,
        earliest_available_date=None,
        expression=checked.expression,
        feature_catalog=HISTORICAL_DAILY_V1.feature_catalog(),
    )
    HISTORICAL_DAILY_V1.require_runnable_definition(definition)
    return definition
