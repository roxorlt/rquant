"""Top-bar stock search and the read-only stock summary over synthetic Serving data."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


@pytest.fixture(scope="module")
def panorama_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("stock-search") / "serving"
    build_web_fixture(root, "panorama")
    return root


def _get(root: Path, path: str, status: int = 200) -> dict:
    app = create_app(
        WebSettings(serving_root=root, stale_after_seconds=1e9),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        response = client.get(path)
    assert response.status_code == status, response.text
    return response.json()


def test_stock_search_matches_code_and_name_with_real_stock_basic(panorama_root: Path) -> None:
    by_code = _get(panorama_root, "/api/v1/stocks/search?q=600001")["data"]
    by_name = _get(panorama_root, "/api/v1/stocks/search?q=样本01")["data"]

    assert by_code["available"] is True
    assert by_code["rows"][0] == {"ts_code": "600001.SH", "name": "样本01"}
    assert by_name["rows"][0] == by_code["rows"][0]
    assert by_code["truncated"] is False
    assert _get(panorama_root, "/api/v1/stocks/search?q=%25")["data"]["rows"] == []
    assert _get(panorama_root, "/api/v1/stocks/search?q=does-not-exist")["data"]["rows"] == []


def test_stock_summary_uses_latest_snapshot_and_pool_marks(panorama_root: Path) -> None:
    first = _get(panorama_root, "/api/v1/stocks/600001.SH/summary")["data"]
    second = _get(panorama_root, "/api/v1/stocks/600005.SH/summary")["data"]
    without_daily = _get(panorama_root, "/api/v1/stocks/600030.SH/summary")["data"]

    assert first["name"] == "样本01"
    assert first["price"] is not None
    assert first["pools"] == ["N 字一池"]
    assert second["pools"] == ["二池盯盘"]
    assert without_daily["price"] is not None
    assert without_daily["pools"] == []
    assert _get(panorama_root, "/api/v1/panorama/stocks/600030.SH/daily")["data"]["bars"] == []


def test_stock_search_degrades_without_a_generation(tmp_path: Path) -> None:
    root = tmp_path / "absent"
    result = _get(root, "/api/v1/stocks/search?q=600001")
    assert result["serving"]["state"] == "unavailable"
    assert result["data"] == {
        "query": "600001",
        "available": False,
        "rows": [],
        "truncated": False,
    }
    summary = _get(root, "/api/v1/stocks/600001.SH/summary")["data"]
    assert (summary["name"], summary["price"], summary["pools"]) == (None, None, [])


def test_stock_summary_rejects_malformed_code(panorama_root: Path) -> None:
    _get(panorama_root, "/api/v1/stocks/600001/summary", status=422)
