import json
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.source import FixtureSource


def test_surge_endpoint(tmp_path: Path):
    d = tmp_path / "snapshots" / "2026-10-12" / "surge"
    d.mkdir(parents=True)
    ev = {"ts_code": "000001.SZ", "name": "平安银行", "time": "09:41:03", "kind": "ratio",
          "ratio": 4.2, "acc1": 2.0, "acc5": 3.1, "pct": 0.031, "price": 11.2, "amount": 3.2e8,
          "at": "2026-10-12T09:41:05"}
    (d / "events.jsonl").write_text(json.dumps(ev, ensure_ascii=False) + "\nbad\n", "utf-8")
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    data = client.get("/api/v1/surge").json()["data"]
    assert data["date"] == "2026-10-12"
    assert data["items"][0]["code"] == "000001.SZ" and data["items"][0]["ratio"] == 4.2
    assert client.get("/api/v1/surge?day=2026-10-09").json()["data"]["items"] == []
    from rquant.research.surge import (
        ACC5_MIN,
        ACC_RATIO_MIN,
        AMOUNT_MIN,
        COOLDOWN_S,
        RATIO_MIN,
        START_M,
        WIN_AMOUNT_MIN,
    )

    assert data["rules"] == {
        "ratio_min": RATIO_MIN,
        "amount_min": AMOUNT_MIN,
        "acc5_min": ACC5_MIN,
        "win_amount_min": WIN_AMOUNT_MIN,
        "acc_ratio_min": ACC_RATIO_MIN,
        "start_m": START_M,
        "cooldown_s": COOLDOWN_S,
    }
