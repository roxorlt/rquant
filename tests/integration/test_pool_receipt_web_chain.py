"""A verified zero-hit run reaches the pool page through a real Serving generation."""

from __future__ import annotations

import os
from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.notification_state import NotificationStateStore
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    SignalPageProjectionProducer,
)
from rquant.serving_publisher import ServingReader
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
from tests.unit.test_pool_membership_publication import _three_day_history
from tests.unit.test_pool_price_serving import _seal_v2, _upgrade
from tests.unit.test_pool_result_publication import (
    NOW,
    TODAY,
    _database,
    _insert_receipt,
    _projections,
    _seal_database_before_cutoff,
    _trading_evidence,
)

_POOL_PROJECTIONS = (
    "pool_definition",
    "screen_run_receipt",
    "canvas_latest_trade_date",
    "canvas_hit",
    "canvas_diagnostic",
    "screen_bounds",
)


def test_verified_zero_hit_reaches_pool_page_from_one_generation(tmp_path: Path) -> None:
    replica = tmp_path / "replica.duckdb"
    _database(replica)
    _insert_receipt(replica, day=TODAY)
    projected = _projections(replica, NOW)

    serving_root = tmp_path / "serving"
    build_web_fixture(
        serving_root,
        "baseline",
        signal_projections=tuple(projected[name] for name in _POOL_PROJECTIONS),
    )
    app = create_app(
        WebSettings(serving_root=serving_root, stale_after_seconds=10_000_000),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/pools")

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["latest_trade_date"] == TODAY.isoformat()
    pool = next(item for item in data["pools"] if item["key"] == "n-shape-pool1")
    assert pool["state"] == "current"
    assert pool["member_count"] == 0
    assert pool["members"] == []
    assert pool["steps"] == []
    assert pool["definition"]["state"] == "available"
    assert pool["result"]["state"] == "current_rules"
    assert pool["result"]["trade_date"] == TODAY.isoformat()
    assert pool["result"]["hit_count"] == 0


def test_v2_zero_hit_replaces_v1_member_generation_at_web_api(tmp_path: Path) -> None:
    replica = tmp_path / "rquant_ro.duckdb"
    _three_day_history(replica)
    state = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(replica, atomically_published=True),
        store=state,
    )
    serving_root = tmp_path / "serving"

    def publish(sequence: int) -> None:
        observed = NOW + timedelta(seconds=sequence)
        producer.publish(observed)
        source = {
            item.table_name: item
            for item in state.serving_snapshot(
                observed_at=observed, history_limit=1
            ).payload.projections
        }
        build_web_fixture(
            serving_root,
            "baseline",
            sequence=sequence,
            signal_projections=tuple(
                source[name] for name in (*_POOL_PROJECTIONS, "pool_membership")
            ),
        )

    publish(0)
    app = create_app(
        WebSettings(serving_root=serving_root, stale_after_seconds=10_000_000),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=2),
        background=False,
    )
    with TestClient(app) as client:
        before = client.get("/api/v1/pools")

        replacement = tmp_path / "next.duckdb"
        _database(replacement)
        _trading_evidence(replacement)
        _insert_receipt(replacement, day=TODAY)
        _upgrade(replacement)
        sealed = _seal_v2(replacement, TODAY)
        _seal_database_before_cutoff(replacement)
        os.replace(replacement, replica)
        publish(1)
        published = {
            item.table_name: item
            for item in state.serving_snapshot(
                observed_at=NOW + timedelta(seconds=1), history_limit=1
            ).payload.projections
        }
        app.state.web.tracker.refresh()
        after = client.get("/api/v1/pools")

    with ServingReader(serving_root).acquire_generation() as lease:
        serving_receipt = lease.connection.execute(
            "SELECT result_version, hit_count "
            "FROM screen_run_receipt WHERE preset_name = 'n-shape-pool1'"
        ).fetchone()
        serving_membership = lease.connection.execute(
            "SELECT status, result_version FROM pool_membership "
            "WHERE pool_name = 'n-shape-pool1' AND row_kind = 'status'"
        ).fetchone()
        assert after.headers["x-rquant-generation"] == lease.manifest.generation_id

    assert before.status_code == after.status_code == 200
    assert before.headers["x-rquant-generation"] != after.headers["x-rquant-generation"]
    old_pool = next(
        item for item in before.json()["data"]["pools"] if item["key"] == "n-shape-pool1"
    )
    new_pool = next(
        item for item in after.json()["data"]["pools"] if item["key"] == "n-shape-pool1"
    )
    assert old_pool["member_count"] == 1
    assert len(old_pool["members"]) == 1
    assert new_pool["member_count"] == 0
    assert new_pool["members"] == []
    assert new_pool["result"]["state"] == "current_rules"
    assert new_pool["result"]["hit_count"] == 0
    assert published["screen_run_receipt"].rows[0]["result_version"] == sealed.result_version
    assert serving_receipt == (sealed.result_version, 0)
    assert serving_membership == ("verified", sealed.result_version)
    membership = next(
        row for row in published["pool_membership"].rows if row["pool_name"] == "n-shape-pool1"
    )
    assert membership["status"] == "verified"
    assert membership["result_version"] == sealed.result_version
