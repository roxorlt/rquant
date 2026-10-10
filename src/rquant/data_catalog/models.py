"""Typed, versioned public shape of the data directory."""

from __future__ import annotations

from enum import StrEnum

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class CatalogModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class CatalogField(CatalogModel):
    key: str
    name: str
    description: str
    data_type: str
    unit: str | None
    is_primary_key: bool


class CatalogDataset(CatalogModel):
    dataset_id: str
    table_name: str
    name: str
    purpose: str
    category: str
    sources: list[str]
    update_note: str
    visibility_note: str
    primary_key: list[str]
    schema_available: bool
    sample_available: bool = False
    fields: list[CatalogField]


class CatalogDocument(CatalogModel):
    version: int = Field(default=1, ge=1)
    datasets: list[CatalogDataset]


class CatalogSummary(CatalogModel):
    dataset_id: str
    name: str
    purpose: str
    category: str
    sources: list[str]
    schema_available: bool


class CatalogList(CatalogModel):
    version: int
    datasets: list[CatalogSummary]


class SampleState(StrEnum):
    AVAILABLE = "available"
    EMPTY = "empty"
    MISSING = "missing"
    UNSUPPORTED = "unsupported"
    UNPUBLISHED = "unpublished"
    STALE = "stale"
    ERROR = "error"


SampleValue = str | int | float | bool | None


class CatalogSample(CatalogModel):
    state: SampleState
    rows: list[dict[str, SampleValue]] = Field(max_length=20)

    @model_validator(mode="after")
    def validate_rows(self) -> CatalogSample:
        if (self.state is SampleState.AVAILABLE) != bool(self.rows):
            raise ValueError("available samples require rows; other states require none")
        return self


class CatalogSamplesDocument(CatalogModel):
    version: int = Field(default=1, ge=1, le=1)
    built_at: AwareDatetime
    datasets: dict[str, CatalogSample]


class CatalogDatasetDetail(CatalogDataset):
    sample_fields: list[CatalogField]
    sample: CatalogSample
