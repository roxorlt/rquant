"""Ownerless public strategy template responses and their controlled rules."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import Field, JsonValue

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.strategy_authoring_commands import StrategyTemplateHead
from rquant.strategy_authoring_projection import StrategyTemplateRecentRun
from rquant.strategy_authoring_source import TemplatePoolReference, TemplateSignalReference
from rquant.strategy_template import StrategyTemplate


class StrategyTemplateItem(RuntimeContractModel):
    strategy_id: str
    name: str
    head: StrategyTemplateHead
    saved_at: AwareUtcDatetime
    entry_kind: Literal["pool", "conditions", "signal"]
    archived: bool
    phase: Literal["未评估", "已有回测"]
    latest_run: StrategyTemplateRecentRun | None = None


class StrategyTemplateCatalogData(RuntimeContractModel):
    availability: Literal["unavailable", "empty", "populated"]
    available_at: AwareUtcDatetime | None
    templates: tuple[StrategyTemplateItem, ...] = Field(max_length=500)
    can_create: bool = False


class StrategyTemplateDetailData(RuntimeContractModel):
    strategy_id: str
    name: str
    head: StrategyTemplateHead
    current_head: StrategyTemplateHead
    rules: StrategyTemplate
    saved_at: AwareUtcDatetime
    change_note: str
    archived: bool
    latest_run: StrategyTemplateRecentRun | None = None
    can_save: bool = False
    can_archive: bool = False
    can_run: bool = False


class StrategyTemplateVersionItem(RuntimeContractModel):
    head: StrategyTemplateHead
    saved_at: AwareUtcDatetime
    change_note: str
    is_head: bool
    latest_run: StrategyTemplateRecentRun | None = None


class StrategyTemplateVersionsData(RuntimeContractModel):
    strategy_id: str
    current_head: StrategyTemplateHead
    versions: tuple[StrategyTemplateVersionItem, ...] = Field(max_length=100)
    next_before_version: int | None = None


class StrategyTemplateConditionChoice(RuntimeContractModel):
    key: str
    label: str
    parameter_schema: dict[str, JsonValue]


class StrategyTemplateSourcesData(RuntimeContractModel):
    availability: Literal["unavailable", "empty", "populated"]
    pools: tuple[TemplatePoolReference, ...]
    signals: tuple[TemplateSignalReference, ...]
    conditions: tuple[StrategyTemplateConditionChoice, ...]
    comparison_fields: tuple[str, ...]
    can_create: bool = False


class StrategyTemplateCommandData(RuntimeContractModel):
    command_id: UUID
    status: Literal["rejected", "pending", "succeeded_waiting_publication", "published", "uncertain"]
    strategy_id: str | None = None
    head: StrategyTemplateHead | None = None
    current_head_updated: bool = False
    message: str
