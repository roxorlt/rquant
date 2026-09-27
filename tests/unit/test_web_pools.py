"""Published pool page reads one verified Serving generation without invented edges/counts."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from rquant.pool_definition_projection import build_pool_definition_rows
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.app import create_app
from rquant.web.screen_catalog import screen_blocks
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _projection(name: str, rows: list[dict]) -> ServingProjectionPayload:
    return ServingProjectionPayload(
        table_name=name,
        available_at=FIXTURE_BUILT_AT - timedelta(seconds=30),
        rows=tuple(rows),
    )


def _canvas_row(name: str, pool_refs: list[str]) -> dict:
    return {
        "name": name,
        "description": "日终观察",
        "pool_refs_json": json.dumps(pool_refs),
        "created_at": "2026-09-24T07:00:00Z",
        "updated_at": "2026-09-24T07:00:00Z",
        "source": "user",
        "command_id": f"cmd-{name}",
        "command_hash": "a" * 64,
        "source_identity_hash": "b" * 64,
        "record_hash": "c" * 64,
        "version_hash": "d" * 64,
    }


def _rule_row(
    name: str,
    *,
    display_name: str | None = None,
    state: str = "available",
    reason: str | None = None,
    rules: list[dict] | None = None,
    depends_on: str | None = None,
    delay_mode: str = "none",
    delay_days: int = 0,
) -> dict:
    return {
        "pool_name": name,
        "display_name": display_name or name.removeprefix("user/"),
        "description": "收盘后观察",
        "source_kind": "user",
        "state": state,
        "reason": reason,
        "version": "v" * 64 if state == "available" else None,
        "command_id": "private-command",
        "command_hash": "h" * 64,
        "depends_on": depends_on,
        "delay_mode": delay_mode,
        "delay_days": delay_days,
        "rules_json": json.dumps(rules if rules is not None else [], ensure_ascii=False),
        "include_columns_json": "[]",
        "can_edit": state == "available",
    }


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
    assert pools["deleted-pool"]["state"] == "unpublished"
    assert pools["deleted-pool"]["member_count"] is None
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


def test_technical_diagnostic_label_does_not_reach_page_copy(tmp_path: Path) -> None:
    root = tmp_path / "technical-step"
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
                        "rule_label": "gt(CLOSE[0])",
                        "remaining_count": 3,
                    }
                ],
            ),
        ),
    )
    pool = next(item for item in _get(root)["data"]["pools"] if item["key"] == "n-shape-pool1")
    assert pool["steps"] == [{"step_index": 1, "label": "筛选步骤 1", "count": 3}]


def test_user_pool_names_and_unknown_keys_remain_distinguishable(tmp_path: Path) -> None:
    root = tmp_path / "names"
    keys = ["user/突破新高", "user/回踩均线", "unknown-alpha", "unknown-beta"]
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection("canvas_definition", [_canvas_row("观察画布", keys)]),
            _projection(
                "screen_bounds",
                [
                    {
                        "preset_name": key,
                        "min_date": "2026-09-23",
                        "max_date": "2026-09-23",
                        "candidate_count": 1,
                    }
                    for key in keys
                ],
            ),
        ),
    )
    pools = {pool["key"]: pool for pool in _get(root)["data"]["pools"]}
    assert pools["user/突破新高"]["name"] == "突破新高"
    assert pools["user/回踩均线"]["name"] == "回踩均线"
    unknown_names = [pools[key]["name"] for key in keys[2:]]
    assert len(set(unknown_names)) == 2
    assert all(name.startswith("选股池 ") and name not in keys for name in unknown_names)


def test_canvas_reference_without_first_published_result_is_unverified(tmp_path: Path) -> None:
    root = tmp_path / "unpublished"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection("canvas_definition", [_canvas_row("新画布", ["user/初选池"])]),
            _projection("screen_bounds", []),
            _projection("canvas_hit", []),
            _projection("canvas_diagnostic", []),
        ),
    )
    pool = next(pool for pool in _get(root)["data"]["pools"] if pool["key"] == "user/初选池")
    assert pool["name"] == "初选池"
    assert pool["state"] == "unpublished"
    assert pool["trade_date"] is None
    assert pool["member_count"] is None


def test_later_canvas_keeps_a_published_pool_at_global_limit(tmp_path: Path) -> None:
    root = tmp_path / "many-canvases"
    first_keys = [f"pool-{index:02d}" for index in range(64)]
    later_key = "pool-64"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection(
                "canvas_definition",
                [
                    _canvas_row("第一画布", first_keys),
                    _canvas_row("第二画布", [later_key]),
                ],
            ),
            _projection(
                "canvas_hit",
                [
                    {
                        "trade_date": "2026-09-23",
                        "preset_name": key,
                        "ts_code": "600001.SH",
                        "row_json": "{}",
                    }
                    for key in [*first_keys, later_key]
                ],
            ),
        ),
    )
    data = _get(root)["data"]
    pools = {pool["key"]: pool for pool in data["pools"]}
    assert data["pools_truncated"] is True
    assert len(data["pools"]) == 64
    assert later_key in pools
    assert pools[later_key]["state"] == "current"
    assert pools[later_key]["member_count"] == 1


def test_published_rules_include_real_conditions_and_keep_results_separate(tmp_path: Path) -> None:
    root = tmp_path / "rules"
    rows = list(build_pool_definition_rows({}, {}, root_path="/synthetic"))
    rows.append(
        _rule_row(
            "user/观察池",
            rules=[
                {"name": "board_in", "args": {"boards": ["main", "gem"]}},
                {"name": "has_lower_shadow", "args": {"min_amplitude": 0.02}},
            ],
            depends_on="n-shape-pool1",
            delay_mode="exact",
            delay_days=2,
        )
    )
    build_web_fixture(root, "baseline", signal_projections=(_projection("pool_definition", rows),))

    body = _get(root)
    data = body["data"]
    assert data["rules_available"] is True
    pools = {pool["key"]: pool for pool in data["pools"]}
    assert pools["n-shape-pool1"]["member_count"] == 3
    assert pools["n-shape-pool2"]["definition"]["depends_on"] == "n-shape-pool1"
    assert pools["n-shape-pool2"]["definition"]["delay_label"] == "使用父池前 2 个交易日内的成员"
    custom = pools["user/观察池"]
    assert custom["state"] == "unpublished"
    assert custom["definition"]["state"] == "available"
    assert custom["definition"]["source_label"] == "自建规则"
    assert custom["definition"]["depends_on"] == "n-shape-pool1"
    assert custom["definition"]["delay_label"] == "使用父池恰好前 2 个交易日的成员"
    assert custom["definition"]["rules"][0] == {
        "label": "所属板块",
        "parameters": [{"label": "板块", "value": "沪深主板、创业板"}],
    }
    assert {
        item["label"]: item["value"] for item in custom["definition"]["rules"][1]["parameters"]
    } == {
        "下影线倍数": "1.5",
        "最小振幅": "2%",
        "相对日期": "所选交易日",
    }
    size_rule = next(
        item
        for item in pools["n-shape-pool1"]["definition"]["rules"]
        if item["label"] == "流通市值低于"
    )
    assert size_rule["parameters"][0] == {"label": "市值上限", "value": "150 亿元"}
    assert "private-command" not in json.dumps(data, ensure_ascii=False)
    assert "command_hash" not in json.dumps(data, ensure_ascii=False)


def test_rule_source_and_result_source_are_independent(tmp_path: Path) -> None:
    members = _get(_published_baseline(tmp_path / "members"))["data"]
    assert members["rules_available"] is False
    assert members["pools"][0]["member_count"] is not None

    root = tmp_path / "rules-only"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection(
                "pool_definition", list(build_pool_definition_rows({}, {}, root_path="/synthetic"))
            ),
            _projection("canvas_latest_trade_date", []),
            _projection("canvas_hit", []),
            _projection("screen_bounds", []),
        ),
    )
    data = _get(root)["data"]
    assert data["state"] == "no_data"
    assert data["rules_available"] is True
    assert data["latest_trade_date"] is None
    assert {pool["key"] for pool in data["pools"]} == {"n-shape-pool1", "n-shape-pool2"}
    assert all(pool["definition"]["state"] == "available" for pool in data["pools"])
    assert all(pool["member_count"] is None for pool in data["pools"])


def _published_baseline(root: Path) -> Path:
    build_web_fixture(root, "baseline")
    return root


def test_invalid_and_oversized_rule_rows_never_look_published(tmp_path: Path) -> None:
    damaged = _rule_row("user/损坏池")
    damaged["rules_json"] = "{"
    rows = [
        _rule_row("user/旧文件", state="migration_required", reason="no_audit"),
        _rule_row("user/缺父池", state="unavailable", reason="parent_missing"),
        _rule_row("user/已删除", state="deleted"),
        damaged,
        _rule_row("user/未知积木", rules=[{"name": "not_registered", "args": {}}]),
        _rule_row("user/过多规则", rules=[{"name": "not_st", "args": {}}] * 65),
    ]
    root = tmp_path / "invalid"
    build_web_fixture(root, "baseline", signal_projections=(_projection("pool_definition", rows),))
    pools = {
        pool["key"]: pool["definition"]
        for pool in _get(root)["data"]["pools"]
        if pool["definition"] is not None
    }

    assert pools["user/旧文件"]["state"] == "migration_required"
    assert pools["user/旧文件"]["reason_label"] == "旧规则尚未完成迁移"
    assert pools["user/缺父池"]["reason_label"] == "父池不存在"
    assert pools["user/已删除"]["state"] == "deleted"
    assert pools["user/损坏池"]["reason_label"] == "规则内容损坏"
    assert pools["user/未知积木"]["reason_label"] == "规则内容无法识别"
    assert pools["user/过多规则"]["state"] == "limit_exceeded"
    assert all(item["depends_on"] is None and item["rules"] == [] for item in pools.values())


def test_every_registered_rule_and_default_parameter_has_readable_copy(tmp_path: Path) -> None:
    calls = [
        {
            "name": block.key,
            "args": {
                parameter.key: parameter.initial
                for parameter in block.parameters
                if parameter.initial is not None
            },
        }
        for block in screen_blocks()
    ]
    root = tmp_path / "all-blocks"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection("pool_definition", [_rule_row("user/全部条件", rules=calls)]),
        ),
    )
    pool = next(item for item in _get(root)["data"]["pools"] if item["key"] == "user/全部条件")
    assert pool["definition"]["state"] == "available"
    rendered = pool["definition"]["rules"]
    assert len(rendered) == len(screen_blocks())
    assert all(
        item["label"] and all(param["value"] for param in item["parameters"]) for item in rendered
    )
    assert all("name" not in item and "args" not in item for item in rendered)


def test_rule_only_pool_list_is_bounded_and_marks_truncation(tmp_path: Path) -> None:
    root = tmp_path / "many-rules"
    rows = [_rule_row(f"user/规则{index:02d}") for index in range(70)]
    build_web_fixture(root, "baseline", signal_projections=(_projection("pool_definition", rows),))
    data = _get(root)["data"]
    assert len(data["pools"]) == 64
    assert data["pools_truncated"] is True


def test_rules_and_members_switch_together_between_generations(tmp_path: Path) -> None:
    root = tmp_path / "switch-rules"
    first_rule = _rule_row("user/观察池", rules=[{"name": "not_st", "args": {}}])
    build_web_fixture(
        root, "baseline", signal_projections=(_projection("pool_definition", [first_rule]),)
    )
    app = create_app(
        WebSettings(serving_root=root, stale_after_seconds=600),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=1, seconds=30),
        background=False,
    )
    with TestClient(app) as client:
        first = client.get("/api/v1/pools")
        second_rule = _rule_row("user/观察池", rules=[{"name": "not_bj", "args": {}}])
        build_web_fixture(
            root,
            "baseline",
            sequence=1,
            signal_projections=(
                _projection("pool_definition", [second_rule]),
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
            ),
        )
        app.state.web.tracker.refresh()
        second = client.get("/api/v1/pools")

    assert first.headers["x-rquant-generation"] != second.headers["x-rquant-generation"]
    before = {pool["key"]: pool for pool in first.json()["data"]["pools"]}
    after = {pool["key"]: pool for pool in second.json()["data"]["pools"]}
    assert before["user/观察池"]["definition"]["rules"][0]["label"] == "排除 ST"
    assert after["user/观察池"]["definition"]["rules"][0]["label"] == "排除北交所"
    assert before["n-shape-pool1"]["member_count"] == 3
    assert after["n-shape-pool1"]["member_count"] == 1
