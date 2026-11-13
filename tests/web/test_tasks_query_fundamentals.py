import subprocess

import pytest
from fastapi.testclient import TestClient

from rquant.fundamentals import fetch, read, save
from rquant.web import tasks as t
from rquant.web.app import create_app
from rquant.web.source import FixtureSource, write_demo_research


@pytest.fixture
def client(tmp_path):
    write_demo_research(tmp_path)
    app = create_app(FixtureSource(), research_root=tmp_path)
    app.state.task_runner = t.demo_runner
    return TestClient(app)


def test_tasks_parse_units_and_timers_and_filter_retired():
    def runner(cmd: list[str]) -> str:
        if cmd[:2] == ["systemctl", "list-units"]:
            return (
                "rquant-web.service loaded active running rQuant web API\n"
                "rquant-runtime-signal-router.service loaded inactive dead retired\n"
                "rquant-page-control.service loaded inactive dead retired\n"
                "rquant-daily.timer loaded active waiting rQuant daily ingest timer\n"
            )
        return t.demo_runner(cmd)

    rows = {r.unit: r for r in t.list_tasks(runner)}
    assert "rquant-web.service" in rows
    assert "rquant-runtime-signal-router.service" not in rows
    assert "rquant-page-control.service" not in rows
    assert rows["rquant-daily.timer"].next_run.startswith("Mon 2026-10-12")
    assert rows["rquant-daily.timer"].last_run.startswith("Fri 2026-10-09")


def test_tail_rejects_foreign_units_and_reports_denied():
    assert t.tail("sshd.service", runner=t.demo_runner).error

    def denied(_):
        raise subprocess.CalledProcessError(1, "journalctl")

    assert "失败" in t.tail("rquant-web.service", runner=denied).error


def test_api_tasks_and_missing_systemctl(client, tmp_path):
    assert client.get("/api/v1/tasks").json()["data"][0]["unit"].startswith("rquant")
    assert client.get("/api/v1/tasks/rquant-daily.service/log").json()["data"]["lines"]

    app = create_app(FixtureSource(), research_root=tmp_path)

    def no_systemctl(_):
        raise FileNotFoundError("systemctl")

    app.state.task_runner = no_systemctl
    assert TestClient(app).get("/api/v1/tasks").json()["data"] == []


def test_fetch_latest_period(tmp_path):
    import pandas as pd

    class Pro:
        def fina_indicator(self, **kw):
            return pd.DataFrame(
                [
                    {"ts_code": kw["ts_code"], "end_date": "20250331", "ann_date": "1", "roe": 1.0},
                    {
                        "ts_code": kw["ts_code"],
                        "end_date": "20250630",
                        "ann_date": "2",
                        "roe": float("nan"),
                    },
                ]
            )

    rows = fetch(Pro(), ["600519.SH"])
    assert rows[0].end_date == "20250630" and rows[0].values["roe"] is None
    save(tmp_path, rows)
    assert read(tmp_path).rows[0].code == "600519.SH"
