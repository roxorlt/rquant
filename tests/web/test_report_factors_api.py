import json
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.factor.store import read_latest
from rquant.web.app import create_app
from rquant.web.source import FixtureSource


def test_report_factors_endpoint_and_screener_merge(tmp_path: Path):
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    assert client.get("/api/v1/report-factors").json()["data"]["results"] is None
    d = tmp_path / "report_factors"
    d.mkdir()
    (d / "results.json").write_text(json.dumps({"factors": {"x": {"name": "X"}}, "catalog": []}))
    (d / "latest.json").write_text(json.dumps({"研报·X": {"000001.SZ": 88.0}}, ensure_ascii=False))
    got = client.get("/api/v1/report-factors").json()["data"]["results"]
    assert got["factors"]["x"]["name"] == "X"
    assert read_latest(tmp_path)["研报·X"]["000001.SZ"] == 88.0
