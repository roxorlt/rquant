"""The web catalog reads only the committed artifact, independently of Serving."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings


def test_list_and_detail_work_without_a_serving_generation(tmp_path: Path) -> None:
    app = create_app(WebSettings(serving_root=tmp_path / "missing"), background=False)
    with TestClient(app) as client:
        listed = client.get("/api/v1/data/catalog")
        detailed = client.get("/api/v1/data/catalog/daily_bar")

    assert listed.status_code == 200
    assert len(listed.json()["data"]["datasets"]) == 23
    assert "fields" not in listed.json()["data"]["datasets"][0]
    assert listed.json()["serving"]["state"] == "ready"
    assert detailed.status_code == 200
    record = detailed.json()["data"]
    assert record["name"] == "股票日线"
    assert record["schema_available"] is True
    assert record["sample_available"] is False
    assert next(field for field in record["fields"] if field["key"] == "pct_chg") == {
        "key": "pct_chg",
        "name": "涨跌幅",
        "description": "相对前收盘价的涨跌百分比",
        "data_type": "DOUBLE",
        "unit": "%",
        "is_primary_key": False,
    }


def test_unknown_id_and_unreadable_artifact_have_clear_responses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = create_app(WebSettings(serving_root=tmp_path / "missing"), background=False)
    with TestClient(app) as client:
        missing = client.get("/api/v1/data/catalog/no_such_dataset")
        assert missing.status_code == 404
        assert missing.json()["detail"] == "找不到这个数据集"

        monkeypatch.setattr("rquant.web.routes.catalog.CATALOG_FILE", tmp_path / "missing.json")
        unavailable = client.get("/api/v1/data/catalog")
        assert unavailable.status_code == 503
        assert unavailable.json()["detail"] == "数据目录暂时不可用"


def test_catalog_uses_the_v2_paths_without_a_legacy_alias(tmp_path: Path) -> None:
    app = create_app(WebSettings(serving_root=tmp_path / "missing"), background=False)
    paths = app.openapi()["paths"]
    assert "/api/v1/data/catalog" in paths
    assert "/api/v1/data/catalog/{dataset}" in paths
    assert "/api/v1/catalog/datasets" not in paths
    assert "/api/v1/catalog/datasets/{dataset_id}" not in paths
