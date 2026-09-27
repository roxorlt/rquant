"""Versioned data directory served from a committed JSON artifact only."""

from __future__ import annotations

from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request

from rquant.data_catalog.models import (
    CatalogDatasetDetail,
    CatalogDocument,
    CatalogList,
    CatalogSample,
    CatalogSamplesDocument,
    CatalogSummary,
    SampleState,
)
from rquant.data_catalog.sample_policy import SAMPLE_FIELDS, public_sample_value, sample_fields
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.security import current_user

router = APIRouter(prefix="/data")
CATALOG_FILE = Path(__file__).resolve().parents[2] / "data_catalog/catalog-v1.json"
MAX_ARTIFACT_BYTES = 5_000_000
MAX_SAMPLE_AGE = timedelta(days=7)


@lru_cache(maxsize=2)
def _load(path: Path) -> CatalogDocument:
    return CatalogDocument.model_validate_json(path.read_text(encoding="utf-8"))


def _catalog() -> CatalogDocument:
    try:
        return _load(CATALOG_FILE)
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="数据目录暂时不可用") from exc


def _envelope(
    data: CatalogList | CatalogDatasetDetail,
) -> Envelope[CatalogList] | Envelope[CatalogDatasetDetail]:
    # The directory is a static release artifact; its availability does not depend on
    # a Serving generation. The shell reports live-data availability separately.
    meta = ServingMeta(
        generation_id=None,
        built_at=None,
        age_seconds=None,
        state=ServingState.READY,
        message=None,
        detail="catalog artifact v1",
    )
    if isinstance(data, CatalogList):
        return Envelope[CatalogList](data=data, serving=meta)
    return Envelope[CatalogDatasetDetail](data=data, serving=meta)


def _sample(request: Request, item: CatalogDatasetDetail) -> CatalogSample:
    path = request.app.state.web.settings.catalog_samples_file
    if path is None:
        return CatalogSample(state=SampleState.UNPUBLISHED, rows=[])
    try:
        if path.stat().st_size > MAX_ARTIFACT_BYTES:
            raise ValueError("sample artifact too large")
        document = CatalogSamplesDocument.model_validate_json(path.read_bytes())
        if set(document.datasets) != set(SAMPLE_FIELDS):
            raise ValueError("sample dataset registry drift")
        age = request.app.state.web.clock() - document.built_at
        if age < -timedelta(minutes=5):
            raise ValueError("sample artifact has a future date")
        if age > MAX_SAMPLE_AGE:
            return CatalogSample(state=SampleState.STALE, rows=[])
        sample = document.datasets[item.dataset_id]
        if sample.state not in {
            SampleState.AVAILABLE,
            SampleState.EMPTY,
            SampleState.MISSING,
            SampleState.UNSUPPORTED,
        }:
            raise ValueError("invalid sample state")
        fields = {field.key: field for field in item.sample_fields}
        for row in sample.rows:
            if set(row) != set(fields):
                raise ValueError("unapproved sample columns")
            for key, value in row.items():
                if public_sample_value(fields[key], value) != value:
                    raise ValueError("unapproved sample value")
        return sample
    except (OSError, ValueError, KeyError):
        return CatalogSample(state=SampleState.ERROR, rows=[])


@router.get("/catalog", response_model=Envelope[CatalogList], summary="数据目录")
def list_datasets(
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[CatalogList]:
    catalog = _catalog()
    return _envelope(
        CatalogList(
            version=catalog.version,
            datasets=[
                CatalogSummary.model_validate(
                    item.model_dump(include=set(CatalogSummary.model_fields))
                )
                for item in catalog.datasets
            ],
        )
    )


@router.get(
    "/catalog/{dataset}",
    response_model=Envelope[CatalogDatasetDetail],
    summary="数据集字段说明",
)
def get_dataset(
    _viewer: Annotated[str | None, Depends(current_user)], dataset: str, request: Request
) -> Envelope[CatalogDatasetDetail]:
    found = next((item for item in _catalog().datasets if item.dataset_id == dataset), None)
    if found is None:
        raise HTTPException(status_code=404, detail="找不到这个数据集")
    selected = sample_fields(found.dataset_id, found.fields)
    detail = CatalogDatasetDetail.model_validate(
        {
            **found.model_dump(),
            "sample_fields": selected,
            "sample": {"state": "unpublished", "rows": []},
        }
    )
    sample = _sample(request, detail)
    result = detail.model_copy(
        update={"sample": sample, "sample_available": sample.state is SampleState.AVAILABLE}
    )
    return _envelope(result)
