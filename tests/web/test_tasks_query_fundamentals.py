import subprocess

import pytest
from fastapi.testclient import TestClient

from rquant.fundamentals import fetch, read, save
from rquant.web import query as q
from rquant.web import tasks as t
from rquant.web.app import create_app
from rquant.web.source import FixtureSource, write_demo_research


@pytest.fixture
def client(tmp_path):
    write_demo_research(tmp_path)
    app = create_app(FixtureSource(), research_root=tmp_path)
    app.state.task_runner = t.demo_runner
    return TestClient(app)


def test_tasks_parse_units_and_timers():
    rows = {r.unit: r for r in t.list_tasks(t.demo_runner)}
    assert rows["rquant-web.service"].sub == "running"
    assert rows["rquant-daily.timer"].next_run.startswith("Mon 2026-10-12")
    assert rows["rquant-daily.timer"].last_run.startswith("Fri 2026-10-09")


def test_tail_rejects_foreign_units_and_reports_denied():
    assert t.tail("sshd.service", runner=t.demo_runner).error
    def denied(_):
        raise subprocess.CalledProcessError(1, "journalctl")
    assert "失败" in t.tail("rquant-web.service", runner=denied).error


@pytest.mark.parametrize("sql", ["DROP TABLE x", "SELECT 1; SELECT 2",
                                 "SELECT * FROM read_csv('/etc/passwd')", "COPY x TO 'f'"])
def test_guard_rejects(sql):
    with pytest.raises(q.QueryError):
        q.guard(sql)


def test_api_tasks_query_fundamentals(client):
    assert client.get("/api/v1/tasks").json()["data"][0]["unit"].startswith("rquant")
    assert client.get("/api/v1/tasks/rquant-daily.service/log").json()["data"]["lines"]
    r = client.post("/api/v1/query", json={"sql": "select ts_code from stock_basic"})
    assert r.status_code == 200 and r.json()["data"]["columns"] == ["ts_code"]
    assert client.post("/api/v1/query", json={"sql": "delete from stock_basic"}).status_code == 400
    assert "stock_basic" in client.get("/api/v1/query/tables").json()["data"]
    doc = client.get("/api/v1/fundamentals").json()["data"]
    assert doc["rows"][0]["code"] == "600519.SH"


def test_viewer_cannot_query(client, monkeypatch):
    monkeypatch.setenv("RQUANT_WEB_ADMINS", "roxor")
    r = client.post("/api/v1/query", json={"sql": "select 1"},
                    headers={"X-Remote-User": "guest"})
    assert r.status_code == 403


def test_fetch_latest_period(tmp_path):
    import pandas as pd

    class Pro:
        def fina_indicator(self, **kw):
            return pd.DataFrame([{"ts_code": kw["ts_code"], "end_date": "20250331", "ann_date": "1",
                                  "roe": 1.0},
                                 {"ts_code": kw["ts_code"], "end_date": "20250630", "ann_date": "2",
                                  "roe": float("nan")}])
    rows = fetch(Pro(), ["600519.SH"])
    assert rows[0].end_date == "20250630" and rows[0].values["roe"] is None
    save(tmp_path, rows)
    assert read(tmp_path).rows[0].code == "600519.SH"


@pytest.mark.parametrize("sql", [
    "select * from secret_table",
    "with x as (select 1) select * from x join other.stock_basic using (ts_code)",
    "select * from duckdb_settings()",
    "select * from information_schema.tables",
    "select getenv('HOME')",
])
def test_guard_table_allowlist_rejects(sql):
    with pytest.raises(q.QueryError):
        q.guard(sql)


def test_guard_allowlist_accepts_cte_and_env_override(monkeypatch):
    assert q.guard("with x as (select * from stock_basic) select * from x, main.signals")
    monkeypatch.setenv("RQUANT_WEB_QUERY_TABLES", "signals")
    with pytest.raises(q.QueryError):
        q.guard("select * from stock_basic")


def test_sandboxed_connection_blocks_file_access(tmp_path):
    import duckdb

    db = tmp_path / "s.duckdb"
    duckdb.connect(str(db)).execute("create table stock_basic as select 1 x").close()
    con = duckdb.connect(str(db), read_only=True)
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    with pytest.raises(duckdb.Error):
        con.execute("SET enable_external_access = true")
