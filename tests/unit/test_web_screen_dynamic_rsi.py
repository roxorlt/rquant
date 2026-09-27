"""The replica screen only offers custom RSI when its matching projection is ready."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.screen.dynamic_rsi import publish_dynamic_rsi_projection
from rquant.screen.replica_source import VerifiedReplicaScreenSource
from rquant.storage.duckdb import DuckDBStore
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.unit.test_web_screen_replica import _publish, _replica_world, _run


def _client(root: Path, primary: Path, replica: Path) -> TestClient:
    return TestClient(
        create_app(
            WebSettings(
                serving_root=root / "absent",
                screen_primary_path=primary,
                screen_replica_path=replica,
                screen_rsi_root=root / "rsi",
            ),
            background=False,
        )
    )


def _catalog(client: TestClient) -> dict:
    response = client.get("/api/v1/screen/blocks")
    assert response.status_code == 200
    return response.json()["data"]


def _period(catalog: dict) -> dict:
    block = next(block for block in catalog["blocks"] if block["key"] == "rsi_overbought")
    return next(parameter for parameter in block["parameters"] if parameter["key"] == "period")


def test_custom_rsi_is_offered_only_for_matching_projection_and_runs_real_rules(
    tmp_path: Path,
) -> None:
    primary, replica, _ = _replica_world(tmp_path, days=70)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar"
        )
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        assert _period(_catalog(client))["input"] == "choice"
        assert (
            _run(
                client,
                conditions=[
                    {"key": "rsi_overbought", "args": {"period": 7, "threshold": 70}},
                ],
            ).status_code
            == 422
        )
    source = VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica)
    publish_dynamic_rsi_projection(source, tmp_path / "rsi")
    with _client(tmp_path, primary, replica) as client:
        catalog = _catalog(client)
        period = _period(catalog)
        assert (period["input"], period["minimum"], period["maximum"]) == ("integer", 2, 60)
        assert period["initial"] == 14
        compare = next(block for block in catalog["blocks"] if block["key"] == "gt")
        assert compare["parameters"][0]["custom_ma"] is True
        for condition in (
            {"key": "rsi_overbought", "args": {"period": 7, "threshold": 70}},
            {"key": "rsi_overbought", "args": {"period": 7, "threshold": 70, "offset": 30}},
            {"key": "gt", "args": {"left": "RSI7[0]", "right": 70}},
            {"key": "between", "args": {"field": "RSI7[0]", "low": 70, "high": 101}},
        ):
            response = _run(client, conditions=[condition], page_size=3)
            assert response.status_code == 200, response.text
            assert response.json()["data"]["total"] == 3
        fixed = _run(
            client,
            conditions=[
                {"key": "rsi_oversold", "args": {"period": 14, "threshold": 70}},
            ],
        )
        assert fixed.status_code == 200
        assert fixed.json()["data"]["total"] == 3
        for condition in (
            {"key": "rsi_overbought", "args": {"period": 7, "threshold": 70, "offset": 31}},
            {"key": "gt", "args": {"left": "RSI7[31]", "right": 70}},
            {"key": "between", "args": {"field": "RSI61[0]", "low": 70, "high": 100}},
        ):
            assert _run(client, conditions=[condition]).status_code == 422

    with DuckDBStore(primary) as store:
        store._conn.execute("UPDATE daily_bar SET close=11 WHERE trade_date=?", [date(2026, 4, 15)])
    _publish(primary, replica)
    with _client(tmp_path, primary, replica) as client:
        assert _period(_catalog(client))["input"] == "choice"
        assert (
            _run(
                client,
                conditions=[
                    {"key": "rsi_overbought", "args": {"period": 7, "threshold": 70}},
                ],
            ).status_code
            == 422
        )


def test_tampered_projection_removes_custom_rsi_without_disabling_fixed_rsi(tmp_path: Path) -> None:
    primary, replica, _ = _replica_world(tmp_path, days=70)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar"
        )
    _publish(primary, replica)
    publish_dynamic_rsi_projection(
        VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica),
        tmp_path / "rsi",
    )
    manifest = json.loads((tmp_path / "rsi" / "current.json").read_text())
    artifact = tmp_path / "rsi" / manifest["file_name"]
    artifact.chmod(0o644)
    artifact.write_bytes(artifact.read_bytes() + b"tampered")
    with _client(tmp_path, primary, replica) as client:
        assert _period(_catalog(client))["input"] == "choice"
        assert (
            _run(
                client,
                conditions=[
                    {"key": "rsi_oversold", "args": {"period": 14, "threshold": 70}},
                ],
            ).status_code
            == 200
        )
        for value in (1, 7, 61, "07", 7.5, True):
            assert (
                _run(
                    client,
                    conditions=[
                        {"key": "rsi_overbought", "args": {"period": value, "threshold": 70}},
                    ],
                ).status_code
                == 422
            )


def test_missing_factor_remains_unknown_in_screen_diagnostics(tmp_path: Path) -> None:
    primary, replica, latest = _replica_world(tmp_path, days=70)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar"
        )
        store._conn.execute(
            "DELETE FROM adj_factor WHERE ts_code='600003.SH' AND trade_date=?",
            [latest],
        )
    _publish(primary, replica)
    publish_dynamic_rsi_projection(
        VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica),
        tmp_path / "rsi",
    )
    with _client(tmp_path, primary, replica) as client:
        response = _run(
            client,
            conditions=[
                {"key": "rsi_overbought", "args": {"period": 7, "threshold": 70}},
            ],
            page_size=3,
        )
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert data["total"] == 2
    assert data["unknown_count"] == 1
    assert data["steps"][0]["unknown_count"] == 1
