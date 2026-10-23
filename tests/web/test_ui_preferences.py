"""Page settings in the web store: per user, allow-listed keys, admin-only writes."""

from __future__ import annotations

import sqlite3

from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.screener_schemes import SqliteSchemeStore, preference_key_ok
from rquant.web.source import FixtureSource


def _client(tmp_path, monkeypatch, admins: str = "") -> tuple[TestClient, object]:
    monkeypatch.setenv("RQUANT_WEB_ADMINS", admins)
    app = create_app(FixtureSource(), dist=None)
    store = SqliteSchemeStore(tmp_path / "web_store.sqlite3")
    app.state.screener_schemes = store
    return TestClient(app), store


def test_schema_has_only_the_two_web_tables(tmp_path) -> None:
    SqliteSchemeStore(tmp_path / "s.sqlite3")
    con = sqlite3.connect(tmp_path / "s.sqlite3")
    names = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert names == {"screener_scheme", "ui_preference"}


def test_round_trip_per_user(tmp_path, monkeypatch) -> None:
    client, _ = _client(tmp_path, monkeypatch)
    me = {"X-Remote-User": "roxor"}
    assert client.get("/api/v1/ui-preferences", headers=me).json()["data"]["prefs"] == {}
    put = client.put("/api/v1/ui-preferences", headers=me,
                     json={"key": "cols.选股结果", "value": "pe,pb"})
    assert put.json()["status"] == "succeeded"
    show = {"key": "pref.monitor.show", "value": '"open"'}
    client.put("/api/v1/ui-preferences", headers=me, json=show)
    prefs = client.get("/api/v1/ui-preferences", headers=me).json()["data"]["prefs"]
    assert prefs == {"cols.选股结果": "pe,pb", "pref.monitor.show": '"open"'}
    other = client.get("/api/v1/ui-preferences", headers={"X-Remote-User": "x"}).json()
    assert other["data"]["prefs"] == {}
    client.put("/api/v1/ui-preferences", headers=me, json={"key": "cols.选股结果", "value": None})
    prefs = client.get("/api/v1/ui-preferences", headers=me).json()["data"]["prefs"]
    assert prefs == {"pref.monitor.show": '"open"'}


def test_keys_are_allow_listed(tmp_path, monkeypatch) -> None:
    client, _ = _client(tmp_path, monkeypatch)
    for key in ("theme", "rail", "screener.schemes", "cols.", "x.y"):
        assert not preference_key_ok(key)
        r = client.put("/api/v1/ui-preferences", json={"key": key, "value": "1"})
        assert r.status_code == 400
    for key in ("cols.量能异动", "sort.告警时间线", "query.history", "pref.kline.subs"):
        assert preference_key_ok(key)
    too_big = {"key": "query.history", "value": "x" * 16_001}
    assert client.put("/api/v1/ui-preferences", json=too_big).status_code == 422


def test_viewer_cannot_write(tmp_path, monkeypatch) -> None:
    client, store = _client(tmp_path, monkeypatch, admins="roxor")
    r = client.put("/api/v1/ui-preferences", headers={"X-Remote-User": "guest"},
                   json={"key": "cols.选股结果", "value": "pe"})
    assert r.status_code == 403
    assert store.preferences("guest") == {}
    guest = client.get("/api/v1/ui-preferences", headers={"X-Remote-User": "guest"})
    assert guest.status_code == 200
