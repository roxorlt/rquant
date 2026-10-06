"""Public collection scope and current seven-interface financial source facts."""
from __future__ import annotations
from datetime import date
from typing import Literal
from pydantic import BaseModel,ConfigDict,Field
from rquant.data_collection_contracts import DatasetCollectionClaim
from rquant.backfill_execute_projection import FinancialSourceProjectionRow


class CollectionDatasetView(BaseModel):
    model_config=ConfigDict(frozen=True,extra='forbid')
    dataset_id: str
    name: str
    status: Literal['verified','partial','unconfirmed']
    status_label: str
    scopes: tuple[DatasetCollectionClaim,...] = Field(max_length=16)
    completed_through: date | None


class FinancialSourceView(FinancialSourceProjectionRow):
    name: str
    permission_label: str


class DataCollectionData(BaseModel):
    model_config=ConfigDict(frozen=True,extra='forbid')
    status: Literal['ready','not_published','unavailable']
    report_hash: str | None = None
    datasets: tuple[CollectionDatasetView,...] = Field(default=(),max_length=24)
    coverage_label: Literal['全市场覆盖尚未核验'] = '全市场覆盖尚未核验'


class FinancialSourcesData(BaseModel):
    model_config=ConfigDict(frozen=True,extra='forbid')
    status: Literal['ready','not_published','unavailable']
    sources: tuple[FinancialSourceView,...] = Field(default=(),max_length=7)
