"""Versioned data directory served from a committed JSON artifact only."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException

from rquant.data_catalog.models import CatalogDataset, CatalogDocument, CatalogList, CatalogSummary
from rquant.web.envelope import Envelope, ServingMeta, ServingState
from rquant.web.security import current_user

router = APIRouter(prefix="/data")
CATALOG_FILE = Path(__file__).resolve().parents[2] / "data_catalog/catalog-v1.json"


@lru_cache(maxsize=2)
def _load(path: Path) -> CatalogDocument:
    return CatalogDocument.model_validate_json(path.read_text(encoding="utf-8"))


def _catalog() -> CatalogDocument:
    try:
        return _load(CATALOG_FILE)
    except (OSError, ValueError) as exc:
        raise HTTPException(status_code=503, detail="数据目录暂时不可用") from exc


def _envelope(
    data: CatalogList | CatalogDataset,
) -> Envelope[CatalogList] | Envelope[CatalogDataset]:
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
    return Envelope[CatalogDataset](data=data, serving=meta)


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
    response_model=Envelope[CatalogDataset],
    summary="数据集字段说明",
)
def get_dataset(
    _viewer: Annotated[str | None, Depends(current_user)], dataset: str
) -> Envelope[CatalogDataset]:
    found = next((item for item in _catalog().datasets if item.dataset_id == dataset), None)
    if found is None:
        raise HTTPException(status_code=404, detail="找不到这个数据集")
    return _envelope(found)
