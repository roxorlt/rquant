"""Wave 3 Mode-A Screener Workbench API & engine tests."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

from rquant.llm.registry import REGISTRY
from rquant.web.app import create_app
from rquant.web.commands_service import handle as handle_command
from rquant.web.screener_schemes import SqliteSchemeStore
from rquant.web.source import FixtureSource, write_demo_research


def _build_client(tmp_path: Path, *, with_llm: bool = False) -> tuple[TestClient, Path]:
    root = tmp_path / "research"
    root.mkdir(parents=True, exist_ok=True)
    write_demo_research(root)

    # Add one ST stock and one BJ stock into market.duckdb for positive/negative rule cases
    con = duckdb.connect(str(root / "market.duckdb"))
    try:
        latest_day = con.execute("SELECT max(trade_date) FROM daily_bar").fetchone()[0]
        con.executemany(
            "INSERT INTO stock_basic VALUES (?, ?, ?, '主板', DATE '2015-01-01', NULL)",
            [
                ("000999.SZ", "*ST演示", "综合"),
                ("830001.BJ", "北交演示", "机械"),
            ],
        )
        con.executemany(
            "INSERT INTO daily_bar "
            "VALUES (?, ?, 10.0, 10.5, 9.8, 10.2, 10.0, 2.0, 100000.0, 102000.0)",
            [
                ("000999.SZ", latest_day),
                ("830001.BJ", latest_day),
            ],
        )
    finally:
        con.close()

    fixture = FixtureSource(today=date.today())
    app = create_app(fixture, dist=None, research_root=root)
    cmds = root / "commands"
    app.state.commands_dir = cmds
    app.state.screener_schemes = SqliteSchemeStore(tmp_path / "web_store.sqlite3")

    def transport(payload: dict) -> dict:
        handle_command(cmds, payload)
        return {"command_id": payload["command_id"], "status": "succeeded"}

    app.state.page_control_transport = transport
    if with_llm:

        def fake_llm(messages: list[dict]) -> str:
            user_msg = messages[-1]["content"]
            if "幻觉" in user_msg:
                return json.dumps(
                    {
                        "blocks": [
                            {"type": "rule", "rule_name": "nonexistent_rule", "args": {}},
                            {"type": "rule", "rule_name": "not_st", "args": {}},
                        ]
                    }
                )
            return json.dumps(
                {
                    "blocks": [
                        {"type": "rule", "rule_name": "not_st", "args": {}},
                        {"type": "field", "field": "roe", "op": ">=", "value": 15.0},
                    ]
                }
            )

        app.state.llm = (fake_llm, "deepseek-test")
    else:
        app.state.llm = None

    return TestClient(app), root


def test_catalog_exposes_all_26_rules_fields_factors_and_upstreams(tmp_path: Path) -> None:
    client, _ = _build_client(tmp_path)
    res = client.get("/api/v1/screener/catalog")
    assert res.status_code == 200
    data = res.json()["data"]

    rule_names = {r["name"] for r in data["rules"]}
    assert rule_names == {spec.name for spec in REGISTRY}
    assert len(data["rules"]) == 26
    assert data["all_market_count"] == 8  # 6 demo stocks + *ST演示 + 北交演示

    groups = {g["group"]: g["items"] for g in data["field_groups"]}
    assert {"行情", "估值", "财务", "资金", "已验证因子"}.issubset(groups)
    fin_fields = {it["field"] for it in groups["财务"]}
    assert {"roe", "grossprofit_margin", "netprofit_margin", "debt_to_assets"}.issubset(fin_fields)
    factor_fields = {it["field"]: it for it in groups["已验证因子"]}
    assert "f:演示动量因子" in factor_fields
    assert factor_fields["f:演示动量因子"]["mean_ic"] is not None


def test_upstream_cascading_set_operations(tmp_path: Path) -> None:
    client, _ = _build_client(tmp_path)

    # Save a custom pool and a watchlist entry
    r_pool = client.post(
        "/api/v1/pools",
        json={
            "name": "白酒与招行",
            "mode": "new",
            "pool_refs": ["breakout"],
            "codes": ["600036.SH", "000858.SZ"],
        },
    )
    assert r_pool.status_code == 200
    r_wl = client.post("/api/v1/watchlist", json={"code": "300750.SZ"})
    assert r_wl.status_code == 200

    # All-market base returns all 8 stocks
    all_res = client.post(
        "/api/v1/screener/run",
        json={"upstream": {"base": "all_market"}, "blocks": [], "ranks": []},
    )
    assert all_res.status_code == 200
    all_codes = {r["code"] for r in all_res.json()["data"]["rows"]}
    assert len(all_codes) == 8
    assert {"600519.SH", "600036.SH", "000999.SZ", "830001.BJ"}.issubset(all_codes)

    # Cascade: preset breakout + union pool:白酒与招行 - exclude watchlist (300750.SZ)
    casc_res = client.post(
        "/api/v1/screener/run",
        json={
            "upstream": {
                "base": "preset",
                "preset": "breakout",
                "include_pools": ["pool:白酒与招行"],
                "exclude_pools": ["watchlist"],
            },
            "blocks": [],
            "ranks": [],
        },
    )
    assert casc_res.status_code == 200
    casc_codes = {r["code"] for r in casc_res.json()["data"]["rows"]}
    assert {"600519.SH", "600036.SH", "000858.SZ"}.issubset(casc_codes)
    assert "300750.SZ" not in casc_codes

    # Intersect: all_market ∩ pool:白酒与招行 -> exact 2 stocks
    inter_res = client.post(
        "/api/v1/screener/run",
        json={
            "upstream": {
                "base": "all_market",
                "intersect_pools": ["pool:白酒与招行"],
            },
            "blocks": [],
            "ranks": [],
        },
    )
    assert {r["code"] for r in inter_res.json()["data"]["rows"]} == {"600036.SH", "000858.SZ"}


def test_builtin_rules_tdx_fundamentals_ranking_and_step_preview(tmp_path: Path) -> None:
    client, _ = _build_client(tmp_path)

    payload = {
        "upstream": {"base": "all_market"},
        "blocks": [
            {"id": "b1", "type": "rule", "rule_name": "not_st", "args": {}},
            {"id": "b2", "type": "rule", "rule_name": "not_bj", "args": {}},
            {"id": "b3", "type": "tdx", "tdx": "C>0"},
            {"id": "b4", "type": "field", "field": "roe", "op": ">=", "value": 10.0},
        ],
        "ranks": [
            {"field": "roe", "weight": 0.6, "desc": True},
            {"field": "f:演示动量因子", "weight": 0.4, "desc": True},
        ],
    }
    res = client.post("/api/v1/screener/run", json=payload)
    assert res.status_code == 200
    data = res.json()["data"]

    steps = data["steps"]
    assert [s["remaining"] for s in steps] == [8, 7, 6, 6, 1]
    assert steps[1]["dropped"] == 1  # dropped *ST演示
    assert steps[2]["dropped"] == 1  # dropped 北交演示
    assert data["active_step"] == 4
    assert [r["code"] for r in data["rows"]] == ["600519.SH"]
    assert data["rows"][0]["grossprofit_margin"] == 91.3
    assert data["rows"][0]["score"] is not None

    # Preview intermediate step 2 (after not_st + not_bj -> 6 non-ST non-BJ stocks)
    prev_res = client.post("/api/v1/screener/run", json={**payload, "preview_step": 2})
    assert prev_res.status_code == 200
    prev_data = prev_res.json()["data"]
    assert prev_data["active_step"] == 2
    prev_codes = {r["code"] for r in prev_data["rows"]}
    assert len(prev_codes) == 6
    assert "000999.SZ" not in prev_codes
    assert "830001.BJ" not in prev_codes

    # Factor top_pct block
    fac_res = client.post(
        "/api/v1/screener/run",
        json={
            "upstream": {"base": "all_market"},
            "blocks": [
                {
                    "id": "f1",
                    "type": "field",
                    "field": "f:演示动量因子",
                    "op": "top_pct",
                    "value": 50,
                }
            ],
            "ranks": [],
        },
    )
    assert fac_res.status_code == 200
    assert len(fac_res.json()["data"]["rows"]) == 3  # ceil(5 scored stocks * 50%) == 3

    # Invalid TDX reports step error cleanly without 500
    bad_tdx = client.post(
        "/api/v1/screener/run",
        json={
            "upstream": {"base": "presets"},
            "blocks": [{"id": "bad", "type": "tdx", "tdx": "UNKNOWN_FUNC(C,5)>0"}],
            "ranks": [],
        },
    )
    assert bad_tdx.status_code == 200
    assert bad_tdx.json()["data"]["steps"][-1]["error"]


def test_custom_blocks_crud_and_composite_execution(tmp_path: Path) -> None:
    client, _ = _build_client(tmp_path)

    # Invalid TDX custom block is rejected with 422
    bad = client.put(
        "/api/v1/screener/blocks/bad-tdx",
        json={"name": "坏公式", "kind": "tdx", "spec": {"tdx": "FOOBAR(C)"}},
    )
    assert bad.status_code == 422

    # Save a valid TDX block and a composite multi-rule block
    r1 = client.put(
        "/api/v1/screener/blocks/my-ma",
        json={"name": "收盘为正", "kind": "tdx", "spec": {"tdx": "C>0"}},
    )
    assert r1.status_code == 200
    assert r1.json()["block_id"] == "my-ma"
    assert r1.json()["spec"]["expression"] == "close>0"

    r2 = client.put(
        "/api/v1/screener/blocks/clean-roe",
        json={
            "name": "纯净高ROE",
            "description": "排除ST与北交且ROE>=10%",
            "kind": "composite",
            "spec": {
                "blocks": [
                    {"id": "s1", "type": "rule", "rule_name": "not_st", "args": {}},
                    {"id": "s2", "type": "rule", "rule_name": "not_bj", "args": {}},
                    {"id": "s3", "type": "field", "field": "roe", "op": ">=", "value": 10.0},
                ]
            },
        },
    )
    assert r2.status_code == 200
    assert r2.json()["block_id"] == "clean-roe"

    listed = client.get("/api/v1/screener/blocks").json()["data"]["blocks"]
    assert len(listed) == 2

    # Execute the saved custom composite block inside /screener/run
    run = client.post(
        "/api/v1/screener/run",
        json={
            "upstream": {"base": "all_market"},
            "blocks": [{"id": "c1", "type": "custom", "custom_id": "clean-roe"}],
            "ranks": [],
        },
    )
    assert run.status_code == 200
    run_data = run.json()["data"]
    assert run_data["steps"][1]["label"] == "★ 纯净高ROE"
    assert [r["code"] for r in run_data["rows"]] == ["600519.SH"]

    # Delete one custom block
    deleted = client.delete("/api/v1/screener/blocks/my-ma")
    assert deleted.status_code == 200
    assert deleted.json()["status"] == "succeeded"
    after = client.get("/api/v1/screener/blocks").json()["data"]["blocks"]
    assert [b["block_id"] for b in after] == ["clean-roe"]


def test_nl_whitebox_compiler_local_and_llm(tmp_path: Path) -> None:
    # 1. Deterministic local white-box parser (no LLM key configured)
    client_local, _ = _build_client(tmp_path / "local", with_llm=False)
    r_complex = client_local.post(
        "/api/v1/screener/from-text",
        json={"text": "排除ST和北交所，近30日无跌停，昨日首板且流通市值小于80亿"},
    )
    assert r_complex.status_code == 200
    blocks = r_complex.json()["blocks"]
    rnames = [b["rule_name"] for b in blocks]
    assert rnames == [
        "not_st",
        "not_bj",
        "no_limit_down_in_window",
        "first_limit_up",
        "circ_mv_lt",
    ]
    assert blocks[3]["args"]["offset"] == 1
    assert blocks[4]["args"]["threshold_yi"] == 80.0

    r_ma = client_local.post(
        "/api/v1/screener/from-text",
        json={"text": "站上20日线且放量一倍"},
    )
    assert r_ma.status_code == 200
    ma_data = r_ma.json()
    assert [b["rule_name"] for b in ma_data["blocks"]] == ["above_ma", "volume_ratio_gte"]
    assert "ts_mean(close,20)" in (ma_data["expression"] or "")

    # 2. Structured LLM JSON compiler + hallucinated rule rejection
    client_llm, _ = _build_client(tmp_path / "llm", with_llm=True)
    r_ok = client_llm.post("/api/v1/screener/from-text", json={"text": "优质白马"})
    assert r_ok.status_code == 200
    ok_data = r_ok.json()
    assert len(ok_data["blocks"]) == 2
    assert ok_data["model"] == "deepseek-test"

    r_hal = client_llm.post("/api/v1/screener/from-text", json={"text": "测试幻觉积木"})
    assert r_hal.status_code == 200
    hal_data = r_hal.json()
    assert [b["rule_name"] for b in hal_data["blocks"]] == ["not_st"]
    assert "nonexistent_rule" in (hal_data["error"] or "")


def test_sqlite_scheme_migration_and_dag_ready_persistence(tmp_path: Path) -> None:
    db_file = tmp_path / "legacy_store.sqlite3"
    con = sqlite3.connect(db_file)
    con.execute(
        "CREATE TABLE screener_scheme ("
        "name TEXT PRIMARY KEY, "
        "filters_json TEXT NOT NULL, "
        "ranks_json TEXT NOT NULL, "
        "saved_at TEXT NOT NULL)"
    )
    con.execute(
        "INSERT INTO screener_scheme VALUES (?, ?, ?, ?)",
        ("旧方案", "[]", "[]", "2026-10-01T00:00:00Z"),
    )
    con.commit()
    con.close()

    store = SqliteSchemeStore(db_file)
    app = create_app(FixtureSource(), dist=None)
    app.state.screener_schemes = store
    client = TestClient(app)

    # Legacy row loads cleanly with default upstream & empty blocks
    got_old = client.get("/api/v1/screener-schemes").json()["data"]["schemes"]
    assert len(got_old) == 1
    assert got_old[0]["name"] == "旧方案"
    assert got_old[0]["upstream"] == {}
    assert got_old[0]["blocks"] == []

    # Save new DAG-ready scheme with upstream + blocks + ranks
    put = client.put(
        "/api/v1/screener-schemes",
        json={
            "name": "全市场首板精选",
            "filters": [],
            "ranks": [{"field": "roe", "weight": 1.0, "desc": True}],
            "upstream": {
                "base": "all_market",
                "include_pools": [],
                "intersect_pools": [],
                "exclude_pools": ["watchlist"],
            },
            "blocks": [
                {"id": "b1", "type": "rule", "rule_name": "not_st", "args": {}},
                {"id": "b2", "type": "tdx", "tdx": "C>MA(C,20)"},
            ],
        },
    )
    assert put.status_code == 200
    saved = {
        s["name"]: s
        for s in client.get("/api/v1/screener-schemes").json()["data"]["schemes"]
    }
    assert "全市场首板精选" in saved
    s_new = saved["全市场首板精选"]
    assert s_new["upstream"]["base"] == "all_market"
    assert s_new["upstream"]["exclude_pools"] == ["watchlist"]
    assert len(s_new["blocks"]) == 2
    assert s_new["blocks"][1]["tdx"] == "C>MA(C,20)"


def test_screener_never_touches_locked_primary_duckdb(tmp_path: Path, monkeypatch) -> None:
    client, _ = _build_client(tmp_path)
    locked_primary = tmp_path / "primary_locked.duckdb"
    duckdb.connect(str(locked_primary)).close()

    # Hold an exclusive write lock on primary_locked.duckdb in a child process
    holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import duckdb, sys, time; "
                f"c = duckdb.connect({str(locked_primary)!r}); "
                "c.execute('CREATE TABLE IF NOT EXISTS t(x INT)'); "
                "sys.stdout.write('LOCKED\\n'); sys.stdout.flush(); "
                "time.sleep(30)"
            ),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "LOCKED"
        monkeypatch.setenv("DUCKDB_PATH", str(locked_primary))
        monkeypatch.setenv("DUCKDB_READONLY_PATH", str(locked_primary))

        cat = client.get("/api/v1/screener/catalog")
        assert cat.status_code == 200
        run = client.post(
            "/api/v1/screener/run",
            json={
                "upstream": {"base": "all_market"},
                "blocks": [{"id": "b1", "type": "rule", "rule_name": "not_st", "args": {}}],
                "ranks": [],
            },
        )
        assert run.status_code == 200
        assert len(run.json()["data"]["rows"]) == 7
    finally:
        holder.terminate()
        holder.wait(timeout=5)
