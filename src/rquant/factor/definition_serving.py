"""Verified read-only projection of current factor definitions."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from rquant.factor.evaluate import FactorDirection
from rquant.factor.registry import FactorDefinitionRegistry, FactorRegistryIdentity
from rquant.strict_json import canonical_json_bytes

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_MAX_DEFINITIONS = 512


class FactorDefinitionServingState(BaseModel):
    """Whether a verified registry is empty, with its current head count."""

    model_config = _IMMUTABLE

    status: Literal["empty", "populated"]
    definition_count: int = Field(ge=0, le=_MAX_DEFINITIONS, strict=True)


class FactorDefinitionServingRow(BaseModel):
    """One current definition head, including archived heads."""

    model_config = _IMMUTABLE

    factor_id: str
    version: int = Field(ge=1, strict=True)
    content_sha256: str = Field(pattern=_SHA256_PATTERN)
    name_zh: str
    category: str
    direction: FactorDirection
    expression: str
    earliest_available_date: date | None
    dependency_columns: tuple[str, ...]
    max_history_window: int = Field(ge=1, strict=True)
    archived: bool


class FactorDefinitionServingSnapshot(BaseModel):
    """Bounded, immutable payload tied to one registry instance and publication time."""

    model_config = _IMMUTABLE

    registry_instance_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    available_at: AwareDatetime
    state: FactorDefinitionServingState
    definitions: tuple[FactorDefinitionServingRow, ...] = Field(max_length=_MAX_DEFINITIONS)
    sha256: str = Field(pattern=_SHA256_PATTERN)

    @model_validator(mode="after")
    def _check_content(self) -> FactorDefinitionServingSnapshot:
        factor_ids = tuple(row.factor_id for row in self.definitions)
        if factor_ids != tuple(sorted(set(factor_ids))):
            raise ValueError("factor definition rows must have sorted, unique IDs")
        if self.state.definition_count != len(self.definitions):
            raise ValueError("factor definition state count differs from rows")
        expected_status = "populated" if self.definitions else "empty"
        if self.state.status != expected_status:
            raise ValueError("factor definition state differs from rows")
        content = self.model_dump(mode="json", exclude={"sha256"})
        if self.sha256 != hashlib.sha256(canonical_json_bytes(content)).hexdigest():
            raise ValueError("factor definition snapshot digest differs from content")
        return self


def project_factor_definition_serving_snapshot(
    registry: FactorDefinitionRegistry,
    *,
    expected_identity: FactorRegistryIdentity,
    available_at: datetime,
) -> FactorDefinitionServingSnapshot:
    """Read one verified registry view and refuse an incomplete projection."""
    records = registry.list_current(
        expected_identity=expected_identity,
        include_archived=True,
        limit=_MAX_DEFINITIONS + 1,
    )
    if len(records) > _MAX_DEFINITIONS:
        raise ValueError("factor definition count exceeds 512")

    definitions = tuple(
        FactorDefinitionServingRow(
            factor_id=record.definition.factor_id,
            version=record.definition.version,
            content_sha256=record.content_sha256,
            name_zh=record.definition.name_zh,
            category=record.definition.category,
            direction=record.definition.direction,
            expression=record.definition.expression,
            earliest_available_date=record.definition.earliest_available_date,
            dependency_columns=record.definition.dependency_columns,
            max_history_window=record.definition.max_history_window,
            archived=record.head.archived,
        )
        for record in records
    )
    state = FactorDefinitionServingState(
        status="populated" if definitions else "empty",
        definition_count=len(definitions),
    )
    fields = {
        "registry_instance_id": expected_identity.instance_id,
        "available_at": available_at,
        "state": state,
        "definitions": definitions,
    }
    unsigned = FactorDefinitionServingSnapshot.model_construct(**fields, sha256="0" * 64)
    digest = hashlib.sha256(
        canonical_json_bytes(unsigned.model_dump(mode="json", exclude={"sha256"}))
    ).hexdigest()
    return FactorDefinitionServingSnapshot(**fields, sha256=digest)
