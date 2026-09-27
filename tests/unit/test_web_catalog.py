"""The web catalog reads only the committed artifact, independently of Serving."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.data_catalog.samples import build_samples
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.unit.test_data_catalog_samples import _source


def test_list_and_detail_work_without_a_serving_generation(tmp_path: Path) -> None:
    app = create_app(WebSettings(serving_root=tmp_path / "missing"), background=False)
    with TestClient(app) as client:
        listed = client.get("/api/v1/data/catalog")
        detailed = client.get("/api/v1/data/catalog/daily_bar")

    assert listed.status_code == 200
    assert len(listed.json()["data"]["datasets"]) == 24
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


def test_limit_up_pool_is_selectable_with_its_real_schema(tmp_path: Path) -> None:
    app = create_app(WebSettings(serving_root=tmp_path / "missing"), background=False)
    with TestClient(app) as client:
        listed = client.get("/api/v1/data/catalog")
        detailed = client.get("/api/v1/data/catalog/limit_up_pool_daily")

    assert "limit_up_pool_daily" in {
        item["dataset_id"] for item in listed.json()["data"]["datasets"]
    }
    assert detailed.status_code == 200
    record = detailed.json()["data"]
    assert record["name"] == "东方财富涨停池"
    assert record["sources"] == ["东方财富"]
    assert record["update_note"] == "有事件时更新"
    assert record["primary_key"] == ["ts_code", "trade_date", "source"]
    assert record["schema_available"] is True
    assert {field["key"] for field in record["fields"]} >= {
        "ts_code",
        "trade_date",
        "source",
        "seal_amount",
        "break_count",
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


def test_detail_rejects_unapproved_sample_values_and_keeps_dictionary(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.duckdb"
    artifact = tmp_path / "samples.json"
    _source(source)
    build_samples(source, artifact)
    app = create_app(
        WebSettings(serving_root=tmp_path / "missing", catalog_samples_file=artifact),
        background=False,
    )
    with TestClient(app) as client:
        good = client.get("/api/v1/data/catalog/stock_status_daily")
        assert good.status_code == 200
        assert good.json()["data"]["sample_available"] is True
        assert good.json()["data"]["sample"]["state"] == "available"
        assert good.json()["data"]["sample_fields"][0]["name"] == "证券或板块代码"
        assert "/private/secrets/price.json" not in good.text
        assert "notifier.admin.shadow.v1" not in good.text

        payload = json.loads(artifact.read_text(encoding="utf-8"))
        payload["datasets"]["stock_status_daily"]["rows"][0]["source_file"] = "/private/secret"
        artifact.write_text(json.dumps(payload), encoding="utf-8")
        bad = client.get("/api/v1/data/catalog/stock_status_daily")
        assert bad.status_code == 200
        assert bad.json()["data"]["sample"]["state"] == "error"
        assert bad.json()["data"]["sample"]["rows"] == []
        assert "/private/secret" not in bad.text
        assert bad.json()["data"]["fields"] == good.json()["data"]["fields"]


def test_stale_and_unconfigured_samples_have_distinct_empty_states(tmp_path: Path) -> None:
    source = tmp_path / "snapshot.duckdb"
    artifact = tmp_path / "samples.json"
    _source(source)
    payload = build_samples(source, artifact)
    payload["built_at"] = "2020-01-01T00:00:00Z"
    artifact.write_text(json.dumps(payload), encoding="utf-8")
    stale = create_app(
        WebSettings(serving_root=tmp_path / "missing", catalog_samples_file=artifact),
        background=False,
    )
    missing = create_app(WebSettings(serving_root=tmp_path / "missing"), background=False)
    with TestClient(stale) as client:
        response = client.get("/api/v1/data/catalog/stock_status_daily")
        assert response.json()["data"]["sample"] == {"state": "stale", "rows": []}
    with TestClient(missing) as client:
        response = client.get("/api/v1/data/catalog/stock_status_daily")
        assert response.json()["data"]["sample"] == {"state": "unpublished", "rows": []}


def test_damaged_sample_file_does_not_break_static_dictionary(tmp_path: Path) -> None:
    artifact = tmp_path / "samples.json"
    artifact.write_text("{broken", encoding="utf-8")
    app = create_app(
        WebSettings(serving_root=tmp_path / "missing", catalog_samples_file=artifact),
        background=False,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/data/catalog/daily_bar")
        assert response.status_code == 200
        assert response.json()["data"]["sample"] == {"state": "error", "rows": []}
        assert response.json()["data"]["fields"]
