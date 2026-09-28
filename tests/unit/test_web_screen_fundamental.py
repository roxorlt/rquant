"""Fundamental screening is offered only from a verified, bound replica receipt."""

from __future__ import annotations

from pathlib import Path

from rquant.storage.duckdb import DuckDBStore
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import build_web_fixture
from tests.unit.test_screen_fundamental_projection import _insert_version, _version
from tests.unit.test_web_screen_replica import _publish, _replica_world

_FIELDS = {
    "PE_TTM[0]": "市盈率（倍）",
    "PB[0]": "市净率（倍）",
    "DV_TTM[0]": "股息率（%）",
    "ROE[0]": "净资产收益率（%）",
    "OR_YOY[0]": "营收同比（%）",
    "NETPROFIT_YOY[0]": "归母净利同比（%）",
}


def _client(tmp_path: Path, primary: Path | None, replica: Path | None) -> TestClient:
    return TestClient(
        create_app(
            WebSettings(
                serving_root=tmp_path / "serving",
                screen_primary_path=primary,
                screen_replica_path=replica,
            ),
            background=False,
        )
    )


def _options(catalog: dict, key: str, parameter: str) -> dict[str, str]:
    block = next(item for item in catalog["blocks"] if item["key"] == key)
    value = next(item for item in block["parameters"] if item["key"] == parameter)
    return {option["value"]: option["label"] for option in value["options"]}


def _run(
    client: TestClient,
    *,
    identity: str | None,
    conditions: list[dict] | None = None,
    cursor: str | None = None,
):
    return client.post(
        "/api/v1/screen/run",
        json={
            "trade_date": "2026-04-15",
            "conditions": conditions or [{"key": "gt", "args": {"left": "PE_TTM[0]", "right": 9}}],
            "source_identity": identity,
            "page_size": 1,
            "cursor": cursor,
        },
        headers={"X-Rquant-Csrf": "1"},
    )


def test_catalog_hides_unpublished_fundamentals_in_replica_and_serving(tmp_path: Path) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    build_web_fixture(tmp_path / "serving", "baseline")
    with _client(tmp_path, primary, replica) as client:
        replica_catalog = client.get("/api/v1/screen/blocks").json()["data"]
        unbound = _run(client, identity=None)
    with _client(tmp_path, None, None) as client:
        serving_catalog = client.get("/api/v1/screen/blocks").json()["data"]
        serving_run = _run(client, identity=None)
    for catalog in (replica_catalog, serving_catalog):
        for key, parameter in (("gt", "left"), ("lt", "right"), ("between", "field")):
            assert set(_FIELDS).isdisjoint(_options(catalog, key, parameter))
    assert unbound.status_code == 422
    assert serving_run.status_code == 422


def test_catalog_and_run_bind_six_fundamentals_to_replica_not_serving(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    build_web_fixture(tmp_path / "serving", "baseline")
    with DuckDBStore(primary) as store:
        version = _version(revision=1)
        _insert_version(store, version)
        store._conn.execute(
            "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?)",
            [version.ts_code, version.trade_date, version.version_id, version.revision],
        )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        response = client.get("/api/v1/screen/blocks")
        catalog = response.json()["data"]
        identity = catalog["source"]["identity"]
        unbound = _run(client, identity=None)
        compare = _run(client, identity=identity)
        interval = _run(
            client,
            identity=identity,
            conditions=[{"key": "between", "args": {"field": "ROE[0]", "low": 10, "high": 15}}],
        )
    for key, parameter in (
        ("gt", "left"),
        ("lt", "right"),
        ("gte", "left"),
        ("lte", "right"),
        ("between", "field"),
    ):
        assert _options(catalog, key, parameter) | _FIELDS == _options(catalog, key, parameter)
        assert all(
            _options(catalog, key, parameter)[field] == label for field, label in _FIELDS.items()
        )
    assert catalog["source_kind"] == "replica"
    assert response.json()["serving"]["generation_id"] != identity
    assert unbound.status_code == 422
    assert compare.status_code == interval.status_code == 200
    assert compare.json()["data"]["source"]["identity"] == identity
    assert compare.json()["data"]["total"] == 1
    assert compare.json()["data"]["unknown_count"] == 2
    assert interval.json()["data"]["total"] == 1


def test_old_catalog_identity_and_cursor_are_rejected_after_replica_rotation(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        version = _version(revision=1)
        _insert_version(store, version)
        store._conn.execute(
            "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?)",
            [version.ts_code, version.trade_date, version.version_id, version.revision],
        )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        identity = client.get("/api/v1/screen/blocks").json()["data"]["source"]["identity"]
        first = _run(client, identity=identity)
        assert first.status_code == 200
        with DuckDBStore(primary) as store:
            store._conn.execute("UPDATE daily_bar SET close = 12 WHERE ts_code = '600001.SH'")
        _publish(primary, replica)
        stale = _run(client, identity=identity)
        stale_page = _run(client, identity=identity, cursor=first.json()["data"]["next_cursor"])
    assert stale.status_code == stale_page.status_code == 409
    assert stale.json()["detail"] == "选股数据已更新，请重新筛选。"
