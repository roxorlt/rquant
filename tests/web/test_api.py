"""Lean web API: every read route answers from the fixture; writes go through page_control."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from rquant.web import page_control
from rquant.web.app import create_app
from rquant.web.source import FixtureSource, SourceUnavailableError


@pytest.fixture
def sent() -> list[dict]:
    return []


@pytest.fixture
def client(sent: list[dict]) -> TestClient:
    app = create_app(FixtureSource(), dist=None)

    def transport(payload: dict) -> dict:
        sent.append(payload)
        return {"command_id": payload["command_id"], "status": "accepted"}

    app.state.page_control_transport = transport
    return TestClient(app)


@pytest.mark.parametrize(
    "path",
    ["/api/v1/meta", "/api/v1/overview", "/api/v1/health", "/api/v1/panorama", "/api/v1/screen",
     "/api/v1/pools", "/api/v1/backtests", "/api/v1/alerts", "/api/v1/paper"],
)
def test_read_routes(client: TestClient, path: str) -> None:
    response = client.get(path)
    assert response.status_code == 200
    assert response.json()["serving"]["state"] == "ready"


def test_screen_filter_and_ranking(client: TestClient) -> None:
    rows = client.get("/api/v1/screen").json()["data"]["rows"]
    assert [r["pct_chg"] for r in rows] == sorted((r["pct_chg"] for r in rows), reverse=True)
    assert client.get("/api/v1/screen", params={"preset": "nope"}).json()["data"]["rows"] == []


def test_pool_members_follow_refs(client: TestClient) -> None:
    pool = client.get("/api/v1/pools").json()["data"]["pools"][0]
    assert pool["pool_refs"] == ["breakout"] and len(pool["members"]) == 4


def test_backtest_detail(client: TestClient) -> None:
    detail = client.get("/api/v1/backtests/run-demo").json()["data"]
    assert len(detail["trades"]) == 4
    assert client.get("/api/v1/backtests/missing").status_code == 404


def test_panorama_pulse(client: TestClient) -> None:
    data = client.get("/api/v1/panorama").json()["data"]
    assert data["pulse"] == {"up": 2, "down": 1, "flat": 1, "limit_up": 0, "limit_down": 0}
    assert {b["board_name"] for b in data["boards"]} == {"白酒", "银行"}


def test_writes_forward_one_command_each(client: TestClient, sent: list[dict]) -> None:
    alert = client.get("/api/v1/alerts").json()["data"]["items"][0]["alert_id"]
    assert client.post("/api/v1/alerts/ack", json={"alert_id": alert}).json()["status"] == (
        "accepted")
    client.post("/api/v1/pools", json={"name": "p1", "pool_refs": ["breakout"]})
    client.post("/api/v1/watchlist", json={"code": "600519.SH"})
    assert [p["kind"] for p in sent] == ["ack_alert", "save_canvas", "add_watchlist_item"]
    assert sent[0]["alert_id"] == alert and len(sent[0]["generation_id"]) == 64


def test_write_validation(client: TestClient, sent: list[dict]) -> None:
    assert client.post("/api/v1/watchlist", json={"code": "bad"}).status_code == 422
    assert client.post("/api/v1/alerts/ack", json={"alert_id": "x"}).status_code == 422
    assert sent == []


def test_page_control_down_is_503() -> None:
    app = create_app(FixtureSource(), dist=None)

    def down(_: dict) -> dict:
        raise OSError("connection refused")

    app.state.page_control_transport = down
    response = TestClient(app).post("/api/v1/watchlist", json={"code": "600519.SH"})
    assert response.status_code == 503


def test_source_unavailable_is_503() -> None:
    class Broken(FixtureSource):
        def query(self, sql, params=()):  # noqa: ANN001, ANN201
            raise SourceUnavailableError("no generation")

    response = TestClient(create_app(Broken(), dist=None)).get("/api/v1/overview")
    assert response.status_code == 503


def test_forward_builds_command() -> None:
    seen: list[dict] = []
    receipt = page_control.forward("save_canvas", {"name": "x"},
                                   lambda p: seen.append(p) or {"status": "accepted"})
    assert receipt == {"status": "accepted"}
    assert seen[0]["kind"] == "save_canvas" and seen[0]["command_id"] and seen[0]["requested_at"]


def test_alert_ack_state_is_read_back() -> None:
    source = FixtureSource()
    client = TestClient(create_app(source, dist=None))
    first = client.get("/api/v1/alerts").json()["data"]["items"][0]
    assert first["acked_at"] is None
    source.record_ack({"alert_id": first["alert_id"], "actor_id": "owner", "command_id": "c1"})
    items = {i["alert_id"]: i for i in client.get("/api/v1/alerts").json()["data"]["items"]}
    assert items[first["alert_id"]]["acked_by"] == "owner"
    assert items[first["alert_id"]]["acked_at"] is not None


def test_meta_carries_deploy_notice(monkeypatch) -> None:
    from fastapi.testclient import TestClient

    from rquant.web.app import create_app
    from rquant.web.source import FixtureSource

    monkeypatch.setenv("RQUANT_WEB_NOTICE", "回放数据 2026-09-25")
    body = TestClient(create_app(FixtureSource(), dist=None)).get("/api/v1/meta").json()
    assert body["data"]["notice"] == "回放数据 2026-09-25"


def test_portfolio_backtests_are_read_from_result_files(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from rquant.web.app import create_app
    from rquant.web.source import FixtureSource, write_demo_research

    write_demo_research(tmp_path)
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    runs = client.get("/api/v1/portfolio-backtests").json()["data"]["runs"]
    assert [r["title"] for r in runs] == ["演示组合回测"]
    detail = client.get(f"/api/v1/portfolio-backtests/{runs[0]['run_id']}").json()["data"]
    assert detail["perf"]["days"] == 4
    assert {o["code"] for o in detail["orders"]} == {"600519.SH", "000001.SZ"}
    assert client.get("/api/v1/portfolio-backtests/nope").status_code == 404


def test_portfolio_compare_and_overfit_block(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from rquant.web.app import create_app
    from rquant.web.source import FixtureSource, write_demo_research

    write_demo_research(tmp_path)
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    run_id = client.get("/api/v1/portfolio-backtests").json()["data"]["runs"][0]["run_id"]
    detail = client.get(f"/api/v1/portfolio-backtests/{run_id}").json()["data"]
    assert detail["overfit"]["trials"] == 1
    both = client.get(f"/api/v1/portfolio-backtests/compare?a={run_id}&b={run_id}").json()
    assert both["data"]["a"]["run"]["run_id"] == both["data"]["b"]["run"]["run_id"]


def test_data_center_lists_catalog_and_audit(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from rquant.web.app import create_app
    from rquant.web.source import FixtureSource, write_demo_research

    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    data = client.get("/api/v1/data-center").json()["data"]
    assert any(d["dataset_id"] == "daily_bar" for d in data["datasets"])
    assert data["audit"] is None
    write_demo_research(tmp_path)
    audit = client.get("/api/v1/data-center").json()["data"]["audit"]
    assert audit["datasets"][0]["missing_open_days"]


def test_factor_results_are_listed_and_read(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from rquant.web.app import create_app
    from rquant.web.source import FixtureSource, write_demo_research

    write_demo_research(tmp_path)
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    items = client.get("/api/v1/factors").json()["data"]["factors"]
    assert [f["name"] for f in items] == ["演示动量因子"]
    detail = client.get(f"/api/v1/factors/{items[0]['factor_id']}").json()["data"]
    assert len(detail["result"]["decay"]) == 6
    assert detail["tracking"]["points"] and items[0]["tracked_to"]
    assert client.get("/api/v1/factors/x").status_code == 404


def test_strategies_list_latest_version(tmp_path) -> None:
    from fastapi.testclient import TestClient

    from rquant.web.app import create_app
    from rquant.web.source import FixtureSource, write_demo_research

    write_demo_research(tmp_path)
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    (row,) = client.get("/api/v1/strategies").json()["data"]["strategies"]
    assert (row["slug"], row["version"]) == ("demo-breakout", 1)
    runs = client.get("/api/v1/portfolio-backtests").json()["data"]["runs"]
    assert runs[0]["strategy"] == "demo-breakout@1"
