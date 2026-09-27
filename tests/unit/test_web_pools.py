"""Published pool page reads one verified Serving generation without invented edges/counts."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _projection(name: str, rows: list[dict]) -> ServingProjectionPayload:
    return ServingProjectionPayload(
        table_name=name,
        available_at=FIXTURE_BUILT_AT - timedelta(seconds=30),
        rows=tuple(rows),
    )


def _get(root: Path) -> dict:
    app = create_app(
        WebSettings(serving_root=root, stale_after_seconds=600),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        response = client.get("/api/v1/pools")
    assert response.status_code == 200
    return response.json()


def test_pools_map_saved_refs_latest_hits_steps_and_older_pools(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection(
                "canvas_definition",
                [
                    {
                        "name": "观察画布",
                        "description": "日终观察",
                        "pool_refs_json": json.dumps(["n-shape-pool1", "old-pool", "deleted-pool"]),
                        "created_at": "2026-09-24T07:00:00Z",
                        "updated_at": "2026-09-24T07:00:00Z",
                        "source": "user",
                        "command_id": "cmd-secret",
                        "command_hash": "a" * 64,
                        "source_identity_hash": "b" * 64,
                        "record_hash": "c" * 64,
                        "version_hash": "d" * 64,
                    }
                ],
            ),
            _projection(
                "screen_bounds",
                [
                    {
                        "preset_name": "n-shape-pool1",
                        "min_date": "2026-09-01",
                        "max_date": "2026-09-23",
                        "candidate_count": 999,
                    },
                    {
                        "preset_name": "old-pool",
                        "min_date": "2026-09-01",
                        "max_date": "2026-09-22",
                        "candidate_count": 200,
                    },
                ],
            ),
            _projection(
                "canvas_diagnostic",
                [
                    {
                        "trade_date": "2026-09-23",
                        "preset_name": "n-shape-pool1",
                        "step_index": 0,
                        "rule_label": "final",
                        "remaining_count": 3,
                    },
                ],
            ),
        ),
    )
    body = _get(root)
    data = body["data"]
    assert data["latest_trade_date"] == "2026-09-23"
    assert data["canvases"] == [
        {
            "name": "观察画布",
            "description": "日终观察",
            "pool_keys": ["n-shape-pool1", "old-pool", "deleted-pool"],
            "refs_truncated": False,
        }
    ]
    pools = {pool["key"]: pool for pool in data["pools"]}
    current = pools["n-shape-pool1"]
    assert (current["state"], current["trade_date"], current["member_count"]) == (
        "current",
        "2026-09-23",
        3,
    )
    assert current["steps"] == [{"step_index": 0, "label": "最终命中", "count": 3}]
    assert [member["code"] for member in current["members"]] == [
        "600002.SH",
        "600004.SH",
        "600006.SH",
    ]
    assert pools["old-pool"]["state"] == "older"
    assert pools["old-pool"]["member_count"] is None
    assert pools["deleted-pool"]["state"] == "missing"
    assert all("999" not in str(pool) and "cmd-secret" not in str(pool) for pool in data["pools"])


def test_pools_empty_and_unavailable_are_explicit(tmp_path: Path) -> None:
    absent = _get(tmp_path / "missing")
    assert absent["serving"]["state"] == "unavailable"
    assert absent["data"]["state"] == "unavailable"
    assert absent["data"]["pools"] == []

    degraded = tmp_path / "degraded"
    build_web_fixture(degraded, "degraded")
    assert _get(degraded)["data"]["state"] == "unavailable"

    empty = tmp_path / "empty"
    build_web_fixture(
        empty,
        "baseline",
        signal_projections=(
            _projection("canvas_hit", []),
            _projection("canvas_latest_trade_date", []),
            _projection("screen_bounds", []),
        ),
    )
    body = _get(empty)
    assert body["data"]["state"] == "no_data"
    assert body["data"]["latest_trade_date"] is None


def test_pools_member_limit_reports_truncation(tmp_path: Path) -> None:
    root = tmp_path / "many"
    hits = [
        {
            "trade_date": "2026-09-23",
            "preset_name": "n-shape-pool1",
            "ts_code": f"{index:06d}.SH",
            "row_json": "{}",
        }
        for index in range(1, 122)
    ]
    build_web_fixture(root, "baseline", signal_projections=(_projection("canvas_hit", hits),))
    pools = {pool["key"]: pool for pool in _get(root)["data"]["pools"]}
    current = pools["n-shape-pool1"]
    assert current["member_count"] == 121
    assert len(current["members"]) == 100
    assert current["members_truncated"] is True


def test_pools_switch_as_one_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    app = create_app(
        WebSettings(serving_root=root, stale_after_seconds=600),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=1, seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        first = client.get("/api/v1/pools")
        build_web_fixture(
            root,
            "baseline",
            sequence=1,
            signal_projections=(
                _projection(
                    "canvas_hit",
                    [
                        {
                            "trade_date": "2026-09-23",
                            "preset_name": "n-shape-pool1",
                            "ts_code": "600001.SH",
                            "row_json": "{}",
                        }
                    ],
                ),
                _projection(
                    "canvas_diagnostic",
                    [
                        {
                            "trade_date": "2026-09-23",
                            "preset_name": "n-shape-pool1",
                            "step_index": 0,
                            "rule_label": "final",
                            "remaining_count": 1,
                        }
                    ],
                ),
            ),
        )
        app.state.web.tracker.refresh()
        second = client.get("/api/v1/pools")
    assert first.status_code == second.status_code == 200
    assert first.headers["x-rquant-generation"] == first.json()["serving"]["generation_id"]
    assert second.headers["x-rquant-generation"] == second.json()["serving"]["generation_id"]
    assert first.headers["x-rquant-generation"] != second.headers["x-rquant-generation"]
    before = {pool["key"]: pool for pool in first.json()["data"]["pools"]}
    after = {pool["key"]: pool for pool in second.json()["data"]["pools"]}
    assert (before["n-shape-pool1"]["member_count"], after["n-shape-pool1"]["member_count"]) == (
        3,
        1,
    )
    assert after["n-shape-pool1"]["steps"] == [{"step_index": 0, "label": "最终命中", "count": 1}]


def test_zero_final_diagnostic_is_current_even_if_bounds_are_older(tmp_path: Path) -> None:
    root = tmp_path / "zero"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection("canvas_hit", []),
            _projection(
                "canvas_diagnostic",
                [
                    {
                        "trade_date": "2026-09-23",
                        "preset_name": "n-shape-pool1",
                        "step_index": 0,
                        "rule_label": "final",
                        "remaining_count": 0,
                    }
                ],
            ),
            _projection(
                "screen_bounds",
                [
                    {
                        "preset_name": "n-shape-pool1",
                        "min_date": "2026-09-01",
                        "max_date": "2026-09-22",
                        "candidate_count": 999,
                    }
                ],
            ),
        ),
    )
    pool = next(item for item in _get(root)["data"]["pools"] if item["key"] == "n-shape-pool1")
    assert pool["state"] == "current"
    assert pool["member_count"] == 0
    assert pool["steps"] == [{"step_index": 0, "label": "最终命中", "count": 0}]


def test_single_named_diagnostic_keeps_its_published_label(tmp_path: Path) -> None:
    root = tmp_path / "named"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection(
                "canvas_diagnostic",
                [
                    {
                        "trade_date": "2026-09-23",
                        "preset_name": "n-shape-pool1",
                        "step_index": 1,
                        "rule_label": "均线过滤",
                        "remaining_count": 3,
                    }
                ],
            ),
        ),
    )
    pool = next(item for item in _get(root)["data"]["pools"] if item["key"] == "n-shape-pool1")
    assert pool["steps"] == [{"step_index": 1, "label": "均线过滤", "count": 3}]
