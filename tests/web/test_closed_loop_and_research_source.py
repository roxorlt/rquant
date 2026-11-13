"""Closed-loop web commands (no Route-A Serving / page_control needed) and ResearchSource."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from rquant.alert_rules import load_alert_rules
from rquant.research.snapshots import build_universe
from rquant.web.app import create_app
from rquant.web.commands_service import handle
from rquant.web.source import (
    FixtureSource,
    ResearchSource,
    SourceUnavailableError,
    write_demo_research,
)


def test_commands_service_decoupled_from_legacy_page_control(tmp_path: Path) -> None:
    import inspect

    from rquant.web import commands_service

    src = inspect.getsource(commands_service)
    assert "from rquant.page_control" not in src
    assert "import rquant.page_control" not in src

    ts = "2026-10-20T08:00:00+00:00"
    code, body = handle(
        tmp_path,
        {
            "kind": "save_canvas",
            "command_id": "cmd-space-1",
            "requested_at": ts,
            "name": "选股 10-20",
            "description": "带空格默认池名",
            "pool_refs": ["breakout"],
            "codes": ["600519.SH", "000001.SZ"],
            "mode": "new",
        },
    )
    assert code == 200 and body["status"] == "succeeded"

    code2, _ = handle(
        tmp_path,
        {
            "kind": "save_canvas",
            "command_id": "cmd-space-2",
            "requested_at": "2026-10-20T08:01:00+00:00",
            "name": "选股 10-20",
            "description": "",
            "pool_refs": ["research_mom20"],
            "codes": ["300750.SZ"],
            "mode": "existing",
        },
    )
    assert code2 == 200
    saved = commands_service.read_saved_pools(tmp_path)
    assert len(saved) == 1
    assert saved[0].name == "选股 10-20"
    assert saved[0].codes == ["600519.SH", "000001.SZ", "300750.SZ"]
    assert saved[0].pool_refs == ["breakout", "research_mom20"]


def test_standalone_write_to_read_closed_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("RQUANT_PAGE_CONTROL_URL", raising=False)
    cmds_dir = tmp_path / "web-commands"
    app = create_app(FixtureSource(), dist=None, commands_dir=cmds_dir)
    client = TestClient(app)

    # 1. Watchlist add -> GET /watchlist -> GET /pools ("自选股") -> DELETE /watchlist
    assert (
        client.post("/api/v1/watchlist", json={"code": "600519.SH", "note": "核心"}).status_code
        == 200
    )
    assert (
        client.post("/api/v1/watchlist", json={"code": "000001.SZ", "note": ""}).status_code == 200
    )
    wl = client.get("/api/v1/watchlist").json()["data"]["items"]
    assert [i["code"] for i in wl] == ["000001.SZ", "600519.SH"]
    pools = client.get("/api/v1/pools").json()["data"]["pools"]
    watch_pool = next(p for p in pools if p["name"] == "自选股")
    assert {m["code"] for m in watch_pool["members"]} == {"600519.SH", "000001.SZ"}

    assert client.delete("/api/v1/watchlist", params={"code": "000001.SZ"}).status_code == 200
    wl_after = client.get("/api/v1/watchlist").json()["data"]["items"]
    assert [i["code"] for i in wl_after] == ["600519.SH"]

    # 2. Save pool with space in name + filtered codes + existing-pool merge
    r_new = client.post(
        "/api/v1/pools",
        json={
            "name": "选股 10-20",
            "description": "筛选命中",
            "pool_refs": ["breakout"],
            "codes": ["600519.SH"],
            "mode": "new",
        },
    )
    assert r_new.status_code == 200 and r_new.json()["status"] == "succeeded"
    r_merge = client.post(
        "/api/v1/pools",
        json={
            "name": "选股 10-20",
            "description": "",
            "pool_refs": ["breakout"],
            "codes": ["300750.SZ"],
            "mode": "existing",
        },
    )
    assert r_merge.status_code == 200
    saved_pool = next(
        p for p in client.get("/api/v1/pools").json()["data"]["pools"] if p["name"] == "选股 10-20"
    )
    assert [m["code"] for m in saved_pool["members"]] == ["600519.SH", "300750.SZ"]

    # 3. Alert rule save -> GET /alert-rules + monitor's load_alert_rules()
    rule_body = {
        "rule_id": "watch-l1",
        "title": "自选一档告警",
        "enabled": True,
        "pools": ["manual", "pool2"],
        "levels": ["L1"],
        "cooldown_minutes": 10,
    }
    assert client.post("/api/v1/alert-rules", json=rule_body).status_code == 200
    rules = client.get("/api/v1/alert-rules").json()["data"]["rules"]
    assert [r["rule_id"] for r in rules] == ["watch-l1"]
    loaded = load_alert_rules(tmp_path / "no-serving", commands_dir=cmds_dir)
    assert len(loaded) == 1 and loaded[0].rule_id == "watch-l1"

    # 4. Retired operations/query routes are absent
    assert client.get("/api/v1/operations").status_code == 404
    assert client.post("/api/v1/query", json={"sql": "SELECT 1"}).status_code == 404


def test_research_source_serves_live_db_without_touching_primary_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    research_dir = tmp_path / "research"
    write_demo_research(research_dir)
    today = date.today()

    # Add screen_result and index_daily to market.duckdb
    mkt = duckdb.connect(str(research_dir / "market.duckdb"))
    try:
        mkt.execute(
            "CREATE TABLE screen_result(trade_date DATE, ts_code VARCHAR, preset_name VARCHAR, "
            "name VARCHAR, close DOUBLE, pct_chg DOUBLE, extra VARCHAR)"
        )
        mkt.execute(
            "INSERT INTO screen_result VALUES (?, '600519.SH', 'research_mom20', '贵州茅台', "
            "1520.0, 2.1, '{\"score\": 30}')",
            [today],
        )
        mkt.execute(
            "CREATE TABLE index_daily("
            "ts_code VARCHAR, trade_date DATE, close DOUBLE, pct_chg DOUBLE)"
        )
        mkt.execute("INSERT INTO index_daily VALUES ('000300.SH', ?, 4010.0, 0.5)", [today])
    finally:
        mkt.close()

    # Create read-only replica rquant_ro.duckdb with monitor_event and pool2 screen_result
    ro_path = tmp_path / "rquant_ro.duckdb"
    ro = duckdb.connect(str(ro_path))
    try:
        ro.execute(
            "CREATE TABLE monitor_event(trade_date DATE, ts_code VARCHAR, level VARCHAR, "
            "trigger_price DOUBLE, level_price DOUBLE, trigger_time TIMESTAMP, "
            "trigger_type VARCHAR, pool VARCHAR)"
        )
        ro.execute(
            "INSERT INTO monitor_event VALUES (?, '600519.SH', 'L1', 1520.0, 1500.0, ?, "
            "'breakout', 'pool2')",
            [today, datetime.combine(today, datetime.min.time()).replace(hour=9, minute=35)],
        )
        ro.execute(
            "CREATE TABLE screen_result(trade_date DATE, preset_name VARCHAR, ts_code VARCHAR, "
            "name VARCHAR, close DOUBLE, pct_chg DOUBLE, extra JSON)"
        )
        ro.execute(
            "INSERT INTO screen_result "
            "VALUES (?, 'pool2', '000001.SZ', '平安银行', 11.2, 1.5, '{}')",
            [today],
        )
    finally:
        ro.close()

    # Hold an exclusive write lock on primary DUCKDB_PATH to prove ResearchSource never opens it
    primary_path = tmp_path / "rquant.duckdb"
    writer_lock = duckdb.connect(str(primary_path))
    writer_lock.execute("CREATE TABLE t(x INT)")
    try:
        monkeypatch.setenv("RQUANT_WEB_SOURCE", "research")
        monkeypatch.setenv("RQUANT_WEB_WARM", "0")
        monkeypatch.setenv("DUCKDB_PATH", str(primary_path))
        monkeypatch.setenv("DUCKDB_READONLY_PATH", str(ro_path))
        monkeypatch.delenv("RQUANT_PAGE_CONTROL_URL", raising=False)

        app = create_app(dist=None, research_root=research_dir, commands_dir=tmp_path / "cmds")
        assert isinstance(app.state.source, ResearchSource)
        client = TestClient(app)

        for route in (
            "/api/v1/meta",
            "/api/v1/overview",
            "/api/v1/health",
            "/api/v1/panorama",
            "/api/v1/screen",
            "/api/v1/pools",
            "/api/v1/alerts",
            "/api/v1/paper",
        ):
            res = client.get(route)
            assert res.status_code == 200, f"{route}: {res.text}"

        ov = client.get("/api/v1/overview").json()["data"]
        kpis = {k["key"]: k["value"] for k in ov["kpis"]}
        assert kpis["candidates"] == "2"
        assert kpis["alerts"] == "1"
        assert kpis["paper_nav"] != "-"
        assert len(ov["signals"]) == 2

        health = client.get("/api/v1/health").json()["data"]
        serving_layer = next(layer for layer in health["layers"] if layer["key"] == "serving")
        assert serving_layer["state"] == "ok"

        pano = client.get("/api/v1/panorama").json()["data"]
        assert {b["system"] for b in pano["boards"]} == {"dc_industry"}
        assert {"白酒", "银行"} <= {b["board_name"] for b in pano["boards"]}

        # No duplicate simulated paper accounts
        paper_accounts = client.get("/api/v1/paper").json()["data"]["accounts"]
        assert [a["account_id"] for a in paper_accounts] == ["sim-demo-breakout"]
    finally:
        writer_lock.close()


def test_research_source_missing_market_db_returns_503(tmp_path: Path) -> None:
    source = ResearchSource(tmp_path / "empty")
    with pytest.raises(SourceUnavailableError):
        source.generation()
    client = TestClient(create_app(source, dist=None))
    assert client.get("/api/v1/meta").status_code == 503


def test_snapshots_universe_uses_commands_watchlist_saved_pools_and_research_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cmds = tmp_path / "cmds"
    ts = "2026-10-20T08:00:00+00:00"
    handle(
        cmds,
        {
            "kind": "add_watchlist_item",
            "command_id": "w1",
            "requested_at": ts,
            "item": {"ts_code": "600519.SH", "note": ""},
        },
    )
    handle(
        cmds,
        {
            "kind": "add_watchlist_item",
            "command_id": "w2",
            "requested_at": ts,
            "item": {"ts_code": "000001.SZ", "note": ""},
        },
    )
    handle(
        cmds,
        {
            "kind": "remove_watchlist_item",
            "command_id": "w3",
            "requested_at": ts,
            "ts_code": "000001.SZ",
        },
    )
    handle(
        cmds,
        {
            "kind": "save_canvas",
            "command_id": "p1",
            "requested_at": ts,
            "name": "选股 10-20",
            "codes": ["300750.SZ"],
        },
    )

    res_root = tmp_path / "research"
    res_root.mkdir()
    con = duckdb.connect(str(res_root / "market.duckdb"))
    try:
        con.execute(
            "CREATE TABLE screen_result(trade_date DATE, ts_code VARCHAR, preset_name VARCHAR)"
        )
        con.execute(
            "INSERT INTO screen_result VALUES (DATE '2026-10-20', '601318.SH', 'research_mom20')"
        )
    finally:
        con.close()

    monkeypatch.delenv("RQUANT_SERVING_ROOT", raising=False)
    monkeypatch.setenv("RQUANT_SNAPSHOT_EXTRA", "")
    monkeypatch.setenv("RQUANT_WEB_COMMANDS_DIR", str(cmds))
    monkeypatch.setenv("RQUANT_RESEARCH_ROOT", str(res_root))
    universe = build_universe()
    assert set(universe["codes"]) == {"600519.SH", "300750.SZ", "601318.SH"}
