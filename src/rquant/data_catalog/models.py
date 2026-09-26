"""Typed, versioned public shape of the data directory."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


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
