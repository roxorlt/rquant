"""ServingSource reads a real (tiny) Serving generation published with main's publisher."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher, ServingTableSpec
from rquant.web.app import create_app
from rquant.web.source import ServingSource


def test_screen_route_over_published_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    built_at = datetime(2026, 10, 9, 7, 0, tzinfo=UTC)
    publisher = ServingPublisher(
        root,
        producer_commit="a" * 40,
        table_specs={
            "stock_basic": ServingTableSpec(sort_keys=("ts_code",)),
            "screen_result": ServingTableSpec(sort_keys=("trade_date", "preset_name", "ts_code")),
        },
    )
    publisher.publish(
        {
            "stock_basic": pd.DataFrame(
                {"ts_code": ["600519.SH"], "name": ["贵州茅台"], "industry": ["白酒"]}),
            "screen_result": pd.DataFrame({
                "trade_date": [date(2026, 10, 9)], "ts_code": ["600519.SH"],
                "preset_name": ["breakout"], "name": ["贵州茅台"], "close": [1500.0],
                "pct_chg": [1.2]}),
        },
        watermarks=tuple(
            ServingDatasetWatermark(
                dataset_id=name, generation_id="src-1", event_time=built_at - timedelta(seconds=1),
                published_at=built_at, sequence=1, status=FreshnessStatus.FRESH)
            for name in ("stock_basic", "screen_result")
        ),
        source_generations={"stock_basic": "src-1", "screen_result": "src-1"},
        built_at=built_at,
    )

    client = TestClient(create_app(ServingSource(root), dist=None))
    body = client.get("/api/v1/screen").json()
    assert body["serving"]["generation_id"]
    assert [r["code"] for r in body["data"]["rows"]] == ["600519.SH"]


def test_missing_generation_is_503(tmp_path: Path) -> None:
    client = TestClient(create_app(ServingSource(tmp_path / "nothing"), dist=None))
    assert client.get("/api/v1/meta").status_code == 503
