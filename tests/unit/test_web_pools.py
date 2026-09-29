"""Published pool page reads one verified Serving generation without invented edges/counts."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from rquant.pool_definition_projection import build_pool_definition_rows
from rquant.screen.loader import BASIC_COLS_MAP, IND_COLS_MAP, PRICE_COLS_MAP, STATE_COLS_MAP
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.app import create_app
from rquant.web.pool_rule_view import _FIELDS
from rquant.web.screen_catalog import screen_blocks
from rquant.web.serving import BorrowedGeneration
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
        "ranking_json": None,
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


def _receipt_row(
    name: str,
    *,
    day: str = "2026-09-23",
    version: str = "v" * 64,
    count: int = 1,
    lineage_complete: bool = True,
    current_definition: bool = True,
) -> tuple[object, ...]:
    return (
        day,
        name,
        version,
        "r" * 64,
        None,
        None,
        count,
        "m" * 64,
        lineage_complete,
        current_definition,
        "2026-09-24T07:00:00Z",
    )


def _membership_status(
    name: str,
    *,
    day: str = "2026-09-23",
    version: str = "r" * 64,
    status: str = "verified",
) -> tuple[object, ...]:
    return (name, day, version, "status", "", status, None, None, None, None)


def _membership_member(
    name: str,
    code: str,
    *,
    day: str = "2026-09-23",
    version: str = "r" * 64,
    status: str = "verified",
    entry_day: str | None = "2026-09-22",
    entry_close: float | None = 10.25,
    entry_version: str | None = "e" * 64,
    reason: str | None = None,
) -> tuple[object, ...]:
    return (
        name,
        day,
        version,
        "member",
        code,
        status,
        entry_day,
        entry_close,
        entry_version,
        reason,
    )


def _receipt_response(
    tmp_path: Path,
    *,
    definitions: list[dict],
    hits: list[tuple[str, str]],
    receipts: list[tuple[object, ...]] | None,
    memberships: list[tuple[object, ...]] | None = None,
    returns: list[tuple[object, ...]] | None = None,
    bounds: dict[str, str] | None = None,
    latest: str = "2026-09-23",
    rank_facts: dict[str, tuple[int, float, str]] | None = None,
) -> dict:
    """Exercise the HTTP route over one synthetic borrowed generation.

    The publisher's receipt table contract is integrated separately; this cursor has
    its projected columns and status so the Web branch can test the consumer now.
    """
    root = tmp_path / "serving"
    manifest = build_web_fixture(root, "baseline")
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            "CREATE TABLE projection_status (table_name VARCHAR, available BOOLEAN, "
            "row_count INTEGER, available_at TIMESTAMPTZ)"
        )
        connection.execute(
            "CREATE TABLE pool_definition (pool_name VARCHAR, display_name VARCHAR, "
            "description VARCHAR, source_kind VARCHAR, state VARCHAR, reason VARCHAR, "
            "version VARCHAR, depends_on VARCHAR, delay_mode VARCHAR, delay_days INTEGER, "
            "rules_json VARCHAR)"
        )
        if definitions:
            connection.executemany(
                "INSERT INTO pool_definition VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    tuple(
                        row[key]
                        for key in (
                            "pool_name",
                            "display_name",
                            "description",
                            "source_kind",
                            "state",
                            "reason",
                            "version",
                            "depends_on",
                            "delay_mode",
                            "delay_days",
                            "rules_json",
                        )
                    )
                    for row in definitions
                ],
            )
        connection.execute(
            "CREATE TABLE canvas_latest_trade_date (snapshot_key VARCHAR, trade_date DATE)"
        )
        connection.execute("INSERT INTO canvas_latest_trade_date VALUES ('current', ?)", [latest])
        connection.execute(
            "CREATE TABLE canvas_hit (trade_date DATE, preset_name VARCHAR, "
            "ts_code VARCHAR, row_json VARCHAR)"
        )
        if hits:
            connection.executemany(
                "INSERT INTO canvas_hit VALUES (?, ?, ?, ?)",
                [
                    (
                        latest,
                        name,
                        code,
                        json.dumps(
                            {
                                "rank_position": rank_facts[code][0],
                                "ranking_score": rank_facts[code][1],
                                "rank_result_version": rank_facts[code][2],
                            }
                            if rank_facts is not None and code in rank_facts
                            else {}
                        ),
                    )
                    for name, code in hits
                ],
            )
        connection.execute("CREATE TABLE screen_bounds (preset_name VARCHAR, max_date DATE)")
        if bounds:
            connection.executemany("INSERT INTO screen_bounds VALUES (?, ?)", list(bounds.items()))
        names = ["pool_definition", "canvas_latest_trade_date", "canvas_hit", "screen_bounds"]
        if receipts is not None:
            connection.execute(
                "CREATE TABLE screen_run_receipt (trade_date DATE, preset_name VARCHAR, "
                "definition_version VARCHAR, result_version VARCHAR, parent_trade_date DATE, "
                "parent_result_version VARCHAR, hit_count INTEGER, member_digest VARCHAR, "
                "lineage_complete BOOLEAN, current_definition BOOLEAN, completed_at TIMESTAMPTZ)"
            )
            if receipts:
                connection.executemany(
                    "INSERT INTO screen_run_receipt VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    receipts,
                )
            names.append("screen_run_receipt")
        if memberships is not None:
            connection.execute(
                "CREATE TABLE pool_membership (pool_name VARCHAR, trade_date DATE, "
                "result_version VARCHAR, row_kind VARCHAR, ts_code VARCHAR, status VARCHAR, "
                "entry_trade_date DATE, entry_close DOUBLE, entry_result_version VARCHAR, "
                "unknown_reason VARCHAR)"
            )
            if memberships:
                connection.executemany(
                    "INSERT INTO pool_membership VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    memberships,
                )
            names.append("pool_membership")
        if returns is not None:
            connection.execute(
                "CREATE TABLE pool_member_return (pool_name VARCHAR, trade_date DATE, "
                "result_version VARCHAR, ts_code VARCHAR, entry_trade_date DATE, "
                "entry_result_version VARCHAR, gain_pct DOUBLE, entry_line_price DOUBLE)"
            )
            if returns:
                connection.executemany(
                    "INSERT INTO pool_member_return VALUES (?, ?, ?, ?, ?, ?, ?, ?)", returns
                )
            names.append("pool_member_return")
        connection.executemany(
            "INSERT INTO projection_status VALUES (?, true, 1, ?)",
            [(name, FIXTURE_BUILT_AT) for name in names],
        )
        app = create_app(
            WebSettings(serving_root=root, stale_after_seconds=600),
            clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
            background=False,
        )
        borrow_count = 0

        @contextmanager
        def borrow() -> Iterator[BorrowedGeneration]:
            nonlocal borrow_count
            borrow_count += 1
            cursor = connection.cursor()
            try:
                yield BorrowedGeneration(manifest, None, cursor, None)
            finally:
                cursor.close()

        with TestClient(app) as client:
            app.state.web.tracker.borrow = borrow
            response = client.get("/api/v1/pools")
        assert response.status_code == 200
        assert borrow_count == 1
        assert response.headers["x-rquant-generation"] == manifest.generation_id
        return response.json()
    finally:
        connection.close()


def test_verified_receipt_confirms_current_rules_and_zero_hit_day(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/命中池"), _rule_row("user/零命中池")],
        hits=[("user/命中池", "600001.SH")],
        receipts=[
            _receipt_row("user/命中池"),
            _receipt_row("user/零命中池", count=0),
        ],
        bounds={"user/命中池": "2026-09-23", "user/零命中池": "2026-09-22"},
    )["data"]
    pools = {pool["key"]: pool for pool in data["pools"]}
    current = pools["user/命中池"]
    assert current["result"] == {
        "state": "current_rules",
        "status_label": "结果已按当前规则更新",
        "trade_date": "2026-09-23",
        "hit_count": 1,
        "zero_hit_label": None,
    }
    assert current["definition"]["status_label"] == "已发布"
    assert [member["code"] for member in current["members"]] == ["600001.SH"]
    zero = pools["user/零命中池"]
    assert zero["result"] == {
        "state": "current_rules",
        "status_label": "结果已按当前规则更新",
        "trade_date": "2026-09-23",
        "hit_count": 0,
        "zero_hit_label": "该交易日没有符合条件的股票",
    }
    assert (zero["state"], zero["trade_date"], zero["member_count"]) == ("current", "2026-09-23", 0)
    assert zero["members"] == [] and zero["steps"] == []


def test_ranked_group_selects_top_hundred_after_full_verification(tmp_path: Path) -> None:
    codes = [f"{600000 + number:06d}.SH" for number in range(101)]
    top_code = codes[-1]
    ranks = {
        code: (1 if code == top_code else index + 2, 100.0 - index / 2, "r" * 64)
        for index, code in enumerate(codes[:-1])
    }
    ranks[top_code] = (1, 100.0, "r" * 64)
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/排名池")],
        hits=[("user/排名池", code) for code in codes],
        receipts=[_receipt_row("user/排名池", count=101)],
        bounds={"user/排名池": "2026-09-23"},
        rank_facts=ranks,
    )["data"]
    pool = next(item for item in data["pools"] if item["key"] == "user/排名池")
    assert pool["member_count"] == 101
    assert pool["members_truncated"] is True
    assert len(pool["members"]) == 100
    assert pool["members"][0]["code"] == top_code
    assert pool["members"][0]["rank_position"] == 1
    assert pool["members"][0]["ranking_score"] == 100.0
    assert pool["members"][-1]["rank_position"] == 100


@pytest.mark.parametrize("bad_group", ("wrong_version", "duplicate", "gap", "missing", "bad_score"))
def test_partial_or_wrong_version_rank_group_exposes_no_scores(
    tmp_path: Path, bad_group: str
) -> None:
    codes = ["600001.SH", "600002.SH", "600003.SH"]
    claims = {
        codes[0]: (1, 95.0, "r" * 64),
        codes[1]: (2, 90.0, "r" * 64),
        codes[2]: (3, 85.0, "r" * 64),
    }
    if bad_group == "wrong_version":
        claims[codes[1]] = (2, 90.0, "x" * 64)
    elif bad_group == "duplicate":
        claims[codes[1]] = (1, 90.0, "r" * 64)
    elif bad_group == "gap":
        claims[codes[1]] = (4, 90.0, "r" * 64)
    elif bad_group == "missing":
        del claims[codes[1]]
    else:
        claims[codes[1]] = (2, 101.0, "r" * 64)
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/排名池")],
        hits=[("user/排名池", code) for code in codes],
        receipts=[_receipt_row("user/排名池", count=3)],
        bounds={"user/排名池": "2026-09-23"},
        rank_facts=claims,
    )["data"]
    pool = next(item for item in data["pools"] if item["key"] == "user/排名池")
    assert [member["code"] for member in pool["members"]] == codes
    assert all(member["rank_position"] is None for member in pool["members"])
    assert all(member["ranking_score"] is None for member in pool["members"])


def test_changed_definition_never_shows_rank_claims(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/排名池")],
        hits=[("user/排名池", "600001.SH")],
        receipts=[_receipt_row("user/排名池", version="b" * 64)],
        bounds={"user/排名池": "2026-09-23"},
        rank_facts={"600001.SH": (1, 95.0, "r" * 64)},
    )["data"]
    pool = next(item for item in data["pools"] if item["key"] == "user/排名池")
    assert pool["result"]["state"] == "rules_changed"
    assert pool["members"][0]["rank_position"] is None


def test_old_serving_pool_definition_without_ranking_column_remains_readable(
    tmp_path: Path,
) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/旧池")],
        hits=[],
        receipts=None,
    )["data"]
    pool = next(item for item in data["pools"] if item["key"] == "user/旧池")
    assert pool["definition"]["state"] == "available"
    assert pool["definition"]["ranking"] is None


def test_zero_hit_copy_says_today_only_on_the_same_calendar_day(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/当日池")],
        hits=[],
        receipts=[_receipt_row("user/当日池", day="2026-09-24", count=0)],
        latest="2026-09-24",
    )["data"]
    pool = next(pool for pool in data["pools"] if pool["key"] == "user/当日池")
    assert pool["result"]["zero_hit_label"] == "今天没有符合条件的股票"


def test_receipt_versions_lineage_and_missing_definition_never_claim_current_rules(
    tmp_path: Path,
) -> None:
    names = ["改规则", "父谱系", "父规则变化", "缺定义", "人数不符", "无回执"]
    definitions = [_rule_row(f"user/{name}") for name in names if name != "缺定义"]
    data = _receipt_response(
        tmp_path,
        definitions=definitions,
        hits=[(f"user/{name}", f"60000{index}.SH") for index, name in enumerate(names)],
        receipts=[
            _receipt_row("user/改规则", version="o" * 64),
            _receipt_row("user/父谱系", lineage_complete=False),
            _receipt_row("user/父规则变化", current_definition=False),
            _receipt_row("user/缺定义"),
            _receipt_row("user/人数不符", count=2),
        ],
    )["data"]
    pools = {pool["key"]: pool for pool in data["pools"]}
    assert pools["user/改规则"]["result"]["status_label"] == "规则已更新，等待下次选股"
    assert pools["user/改规则"]["result"]["state"] == "rules_changed"
    for name in ("父谱系", "父规则变化", "缺定义", "人数不符", "无回执"):
        assert pools[f"user/{name}"]["result"]["state"] == "unverified"
        assert pools[f"user/{name}"]["result"]["status_label"] == "结果版本待确认"
    assert all(pool["members"] for pool in pools.values())


def test_other_pool_today_does_not_relabel_this_pools_older_receipt(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/今日池"), _rule_row("user/前日池")],
        hits=[("user/今日池", "600001.SH")],
        receipts=[
            _receipt_row("user/今日池"),
            _receipt_row("user/前日池", day="2026-09-22", count=2),
        ],
        bounds={"user/今日池": "2026-09-23", "user/前日池": "2026-09-22"},
    )["data"]
    pools = {pool["key"]: pool for pool in data["pools"]}
    assert pools["user/前日池"]["state"] == "older"
    assert pools["user/前日池"]["members"] == []
    assert pools["user/前日池"]["result"] == {
        "state": "older_rules",
        "status_label": "上次结果与当前规则一致",
        "trade_date": "2026-09-22",
        "hit_count": 2,
        "zero_hit_label": None,
    }
    assert pools["user/今日池"]["result"]["state"] == "current_rules"


def test_newer_pool_result_without_receipt_does_not_confirm_an_older_run(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/今日池"), _rule_row("user/未确认池")],
        hits=[("user/今日池", "600001.SH")],
        receipts=[
            _receipt_row("user/今日池", day="2026-09-24"),
            _receipt_row("user/未确认池", day="2026-09-22", count=2),
        ],
        bounds={"user/今日池": "2026-09-24", "user/未确认池": "2026-09-23"},
        latest="2026-09-24",
    )["data"]
    pool = next(pool for pool in data["pools"] if pool["key"] == "user/未确认池")
    assert pool["state"] == "older"
    assert pool["trade_date"] == "2026-09-23"
    assert pool["result"]["state"] == "unverified"
    assert pool["result"]["status_label"] == "结果版本待确认"
    assert pool["result"]["trade_date"] is None
    assert pool["result"]["hit_count"] is None


def test_legacy_members_stay_visible_without_a_receipt(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/旧池")],
        hits=[("user/旧池", "600001.SH")],
        receipts=None,
    )["data"]
    pool = next(pool for pool in data["pools"] if pool["key"] == "user/旧池")
    assert pool["state"] == "current"
    assert [item["code"] for item in pool["members"]] == ["600001.SH"]
    assert pool["result"]["state"] == "unverified"
    assert pool["result"]["status_label"] == "结果版本待确认"


def test_membership_entry_uses_only_matching_current_result_and_member_set(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/可证池")],
        hits=[("user/可证池", "600001.SH"), ("user/可证池", "600002.SH")],
        receipts=[_receipt_row("user/可证池", count=2)],
        memberships=[
            _membership_status("user/可证池"),
            _membership_member("user/可证池", "600001.SH"),
            _membership_member(
                "user/可证池",
                "600002.SH",
                entry_close=None,
                reason="entry_price_missing",
            ),
        ],
    )["data"]
    pool = next(pool for pool in data["pools"] if pool["key"] == "user/可证池")
    assert pool["result"]["state"] == "current_rules"
    members = {member["code"]: member for member in pool["members"]}
    assert members["600001.SH"]["entry_trade_date"] == "2026-09-22"
    assert members["600001.SH"]["entry_close"] == 10.25
    assert members["600002.SH"]["entry_trade_date"] == "2026-09-22"
    assert members["600002.SH"]["entry_close"] is None


@pytest.mark.parametrize(
    "memberships",
    [
        None,
        [
            _membership_status("user/可证池", version="other"),
            _membership_member("user/可证池", "600001.SH"),
        ],
        [
            _membership_status("user/可证池", day="2026-09-22"),
            _membership_member("user/可证池", "600001.SH"),
        ],
        [_membership_status("user/可证池"), _membership_member("user/错池", "600001.SH")],
        [_membership_member("user/可证池", "600001.SH")],
        [_membership_status("user/可证池"), _membership_member("user/可证池", "600002.SH")],
        [
            _membership_status("user/可证池"),
            _membership_member("user/可证池", "600001.SH", version="other"),
        ],
        [
            _membership_status("user/可证池", status="calendar_incomplete"),
            _membership_member("user/可证池", "600001.SH"),
        ],
    ],
)
def test_membership_missing_or_wrong_identity_never_adds_an_entry(
    tmp_path: Path, memberships: list[tuple[object, ...]] | None
) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/可证池")],
        hits=[("user/可证池", "600001.SH")],
        receipts=[_receipt_row("user/可证池")],
        memberships=memberships,
    )["data"]
    pool = next(pool for pool in data["pools"] if pool["key"] == "user/可证池")
    assert pool["result"]["state"] == "current_rules"
    assert pool["members"][0]["entry_trade_date"] is None
    assert pool["members"][0]["entry_close"] is None


def test_membership_unknown_reason_and_invalid_price_never_become_evidence(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/可证池")],
        hits=[("user/可证池", "600001.SH"), ("user/可证池", "600002.SH")],
        receipts=[_receipt_row("user/可证池", count=2)],
        memberships=[
            _membership_status("user/可证池"),
            _membership_member(
                "user/可证池", "600001.SH", entry_close=None, reason="unknown_new_reason"
            ),
            _membership_member("user/可证池", "600002.SH", entry_close=float("nan")),
        ],
    )["data"]
    pool = next(pool for pool in data["pools"] if pool["key"] == "user/可证池")
    for member in pool["members"]:
        assert member["entry_trade_date"] is None
        assert member["entry_close"] is None


def test_membership_entry_does_not_attach_to_an_unverified_result(tmp_path: Path) -> None:
    data = _receipt_response(
        tmp_path,
        definitions=[_rule_row("user/旧规则池")],
        hits=[("user/旧规则池", "600001.SH")],
        receipts=[_receipt_row("user/旧规则池", version="o" * 64)],
        memberships=[
            _membership_status("user/旧规则池"),
            _membership_member("user/旧规则池", "600001.SH"),
        ],
    )["data"]
    pool = next(pool for pool in data["pools"] if pool["key"] == "user/旧规则池")
    assert pool["result"]["state"] == "rules_changed"
    assert pool["members"][0]["entry_trade_date"] is None
    assert pool["members"][0]["entry_close"] is None


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


def test_ranked_pool_rule_detail_displays_plan_without_claiming_scores(tmp_path: Path) -> None:
    row = _rule_row("user/排名观察")
    row["ranking_json"] = json.dumps(
        {
            "conditions": [
                {"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 50},
                {"metric": "CIRC_MV[0]", "ascending": True, "weight": 30},
                {"metric": "PCT_CHG[0]", "ascending": False, "weight": 20},
            ],
            "top_n": 20,
        }
    )
    root = tmp_path / "ranked-rules"
    build_web_fixture(root, "baseline", signal_projections=(_projection("pool_definition", [row]),))

    pool = next(item for item in _get(root)["data"]["pools"] if item["key"] == "user/排名观察")
    assert pool["state"] == "unpublished"
    assert pool["definition"]["ranking"] == {
        "conditions": [
            {"label": "20 日涨幅", "direction_label": "越高越好", "weight": 50.0},
            {"label": "流通市值", "direction_label": "越低越好", "weight": 30.0},
            {"label": "今日涨跌幅", "direction_label": "越高越好", "weight": 20.0},
        ],
        "top_n": 20,
    }
    assert "ranking_score" not in json.dumps(pool)


def test_corrupt_published_ranking_does_not_look_available(tmp_path: Path) -> None:
    row = _rule_row("user/排名损坏")
    row["ranking_json"] = json.dumps(
        {"conditions": [{"metric": "OTHER", "ascending": False, "weight": 100}], "top_n": 20}
    )
    root = tmp_path / "corrupt-ranking"
    build_web_fixture(root, "baseline", signal_projections=(_projection("pool_definition", [row]),))

    pool = next(item for item in _get(root)["data"]["pools"] if item["key"] == "user/排名损坏")
    assert pool["definition"]["state"] == "unavailable"
    assert pool["definition"]["reason_label"] == "排名设置损坏"


def test_unranked_v3_definition_is_available_without_ranking_summary(tmp_path: Path) -> None:
    row = _rule_row("user/无排名新池")
    row["ranking_json"] = "null"
    root = tmp_path / "unranked-v3"
    build_web_fixture(root, "baseline", signal_projections=(_projection("pool_definition", [row]),))

    pool = next(item for item in _get(root)["data"]["pools"] if item["key"] == "user/无排名新池")
    assert pool["definition"]["state"] == "available"
    assert pool["definition"]["ranking"] is None


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
        _rule_row("user/损坏文件", state="unavailable", reason="invalid_content"),
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
    assert pools["user/损坏文件"]["reason_label"] == "规则文件内容损坏"
    assert pools["user/缺父池"]["reason_label"] == "父池不存在"
    assert pools["user/已删除"]["state"] == "deleted"
    assert pools["user/损坏池"]["reason_label"] == "规则内容损坏"
    assert pools["user/未知积木"]["reason_label"] == "规则内容无法识别"
    assert pools["user/过多规则"]["state"] == "limit_exceeded"
    assert all(item["depends_on"] is None and item["rules"] == [] for item in pools.values())


def test_every_executable_wide_field_is_readable(tmp_path: Path) -> None:
    fields = set().union(
        PRICE_COLS_MAP.values(),
        IND_COLS_MAP.values(),
        STATE_COLS_MAP.values(),
        BASIC_COLS_MAP.values(),
    )
    assert set(_FIELDS) == fields
    calls = [
        {"name": "gt", "args": {"left": f"{field}[1]", "right": 1}} for field in sorted(fields)
    ]
    root = tmp_path / "all-fields"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection("pool_definition", [_rule_row("user/全部字段", rules=calls)]),
        ),
    )
    definition = next(
        item["definition"] for item in _get(root)["data"]["pools"] if item["key"] == "user/全部字段"
    )
    assert definition["state"] == "available"
    assert len(definition["rules"]) == len(fields)
    names = {
        field: item["parameters"][0]["value"]
        for field, item in zip(sorted(fields), definition["rules"], strict=True)
    }
    assert names["AMOUNT"] == "前 1 个交易日成交额"
    assert names["PRE_CLOSE"] == "前 1 个交易日前收盘价"
    assert names["TOTAL_MV"] == "前 1 个交易日总市值"
    assert names["MACD_HIST"] == "前 1 个交易日 MACD 柱"
    assert names["KDJ_J"] == "前 1 个交易日 KDJ J 值"


def test_rule_number_copy_preserves_small_thresholds_and_common_format(tmp_path: Path) -> None:
    values = [1e-9, 12345.6789012345, 1000000.0, -0.0025, 0.0]
    calls = [{"name": "gt", "args": {"left": "CLOSE[0]", "right": value}} for value in values]
    root = tmp_path / "numeric-thresholds"
    build_web_fixture(
        root,
        "baseline",
        signal_projections=(
            _projection("pool_definition", [_rule_row("user/精度池", rules=calls)]),
        ),
    )
    definition = next(
        item["definition"] for item in _get(root)["data"]["pools"] if item["key"] == "user/精度池"
    )
    assert definition["state"] == "available"
    assert [item["parameters"][1]["value"] for item in definition["rules"]] == [
        "0.000000001",
        "12,345.6789012345",
        "1,000,000",
        "-0.0025",
        "0",
    ]


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
