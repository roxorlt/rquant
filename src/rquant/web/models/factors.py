"""Public, read-only factor definition catalog response."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.factor.evaluate import FactorDirection
from rquant.factor.registry import FactorHeadRef
from rquant.runtime_contracts import AwareUtcDatetime


class FactorDefinitionItem(BaseModel):
    model_config = ConfigDict(frozen=True)

    factor_id: str
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    name_zh: str
    category_label: str
    direction: FactorDirection
    direction_label: str
    version: int
    earliest_available_date: date | None
    archived: bool
    expression: str
    dependency_columns: list[str]
    max_history_window: int


class FactorCatalogData(BaseModel):
    model_config = ConfigDict(frozen=True)

    availability: Literal["unavailable", "empty", "populated"]
    available_at: datetime | None
    definitions: list[FactorDefinitionItem]
    can_archive: bool = False


class FactorArchiveCommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    requested_at: AwareUtcDatetime
    expected_head: FactorHeadRef


class FactorArchiveCommandData(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    status: Literal[
        "rejected", "pending", "succeeded_waiting_publication", "published", "unavailable"
    ]
    command_id: str
    factor_id: str
    version: int
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    current_head_updated: bool = False
    message: str
