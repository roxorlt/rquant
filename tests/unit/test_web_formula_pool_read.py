"""Formula pools are read from one Serving generation and sealed daily files."""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from rquant.formula_pool_daily import FormulaPoolDailyResultV1, _run_identity
from rquant.formula_pool_serving_projection import (
    FormulaPoolDefinitionRow,
    FormulaPoolLatestResultRow,
    FormulaPoolStateRow,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_read_models import ServingProjectionPayload
from rquant.strict_json import canonical_json_bytes
from rquant.web.app import create_app
from rquant.web.formula_pool_read import read_formula_pool_snapshot
from rquant.web.serving import GenerationTracker
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

PATH = "/api/v1/pools/formula"
HEADERS = {"x-rquant-user": "researcher"}
DAY = date(2026, 9, 23)
VERSION = "a" * 64
UNIVERSE = "b" * 64
PROJECTION = "c" * 64
TASK = "d" * 32
REQUEST = "e" * 64
RESULT = "f" * 64
CODES = ("000001.SZ", "600001.SH")


def _daily(
    match_codes: tuple[str, ...] = CODES,
    *,
    base_name: str = "research",
    unknown_reasons: dict[str, int] | None = None,
    no_match_count: int = 0,
) -> FormulaPoolDailyResultV1:
    reasons = {"missing_date": 1} if unknown_reasons is None else unknown_reasons
    pool_name = f"user/{base_name}"
    content = {
        "schema_version": 1,
        "pool_name": pool_name,
        "definition_version": VERSION,
        "trade_date": DAY,
        "run_identity": _run_identity(pool_name, VERSION, DAY, UNIVERSE, PROJECTION),
        "task_id": TASK,
        "request_sha256": REQUEST,
        "result_sha256": RESULT,
        "universe_identity": UNIVERSE,
        "projection_identity": PROJECTION,
        "market_total": len(match_codes) + no_match_count + sum(reasons.values()),
        "match_count": len(match_codes),
        "no_match_count": no_match_count,
        "unknown_count": sum(reasons.values()),
        "unknown_reasons": reasons,
        "match_codes": match_codes,
        "member_sha256": canonical_sha256(match_codes),
    }
    return FormulaPoolDailyResultV1(**content, content_sha256=canonical_sha256(content))


def _publish(
    serving: Path,
    *,
    sequence: int,
    state: str = "ready",
    run: bool = True,
    daily: FormulaPoolDailyResultV1 | None = None,
    base_name: str = "research",
) -> None:
    at = FIXTURE_BUILT_AT + timedelta(minutes=sequence) - timedelta(minutes=1)
    definitions = (
        ()
        if state == "empty"
        else (
            FormulaPoolDefinitionRow(
                pool_name=f"user/{base_name}",
                display_name="研究池",
                formula="CLOSE>2",
                syntax_version="tdx-v1",
                version=VERSION,
                command_id="save-research",
                command_hash="1" * 64,
                created_at=FIXTURE_BUILT_AT - timedelta(minutes=5),
                creation_task_id="2" * 32,
                creation_result_sha256="3" * 64,
                creation_trade_date=DAY,
            ),
        )
    )
    daily = _daily(base_name=base_name) if daily is None else daily
    latest = (
        ()
        if not run or state == "empty"
        else (
            FormulaPoolLatestResultRow(
                pool_name=daily.pool_name,
                definition_version=daily.definition_version,
                trade_date=daily.trade_date,
                task_id=daily.task_id,
                request_sha256=daily.request_sha256,
                result_sha256=daily.result_sha256,
                universe_identity=daily.universe_identity,
                projection_identity=daily.projection_identity,
                market_total=daily.market_total,
                match_count=daily.match_count,
                no_match_count=daily.no_match_count,
                unknown_count=daily.unknown_count,
                unknown_reasons_json=canonical_json_bytes(daily.unknown_reasons).decode(),
                member_sha256=daily.member_sha256,
                relative_path=f"{base_name}/{DAY.isoformat()}.json",
                content_sha256=daily.content_sha256,
                byte_count=len(canonical_json_bytes(daily.model_dump(mode="json"))),
            ),
        )
    )
    projections = (
        ServingProjectionPayload(
            table_name="formula_pool_state",
            available_at=at,
            rows=(
                FormulaPoolStateRow(
                    availability="empty" if state == "empty" else "ready",
                    pool_count=len(definitions),
                    run_count=len(latest),
                ).model_dump(mode="json"),
            ),
        ),
        ServingProjectionPayload(
            table_name="formula_pool_definition",
            available_at=at,
            rows=tuple(item.model_dump(mode="json") for item in definitions),
        ),
        ServingProjectionPayload(
            table_name="formula_pool_latest_result",
            available_at=at,
            rows=tuple(item.model_dump(mode="json") for item in latest),
        ),
    )
    build_web_fixture(serving, "baseline", sequence=sequence, formula_pool_projections=projections)


def _file(root: Path, daily: FormulaPoolDailyResultV1 | None = None) -> Path:
    chosen = _daily() if daily is None else daily
    directory = root / chosen.pool_name.removeprefix("user/")
    directory.mkdir(parents=True, mode=0o700)
    result = directory / f"{DAY.isoformat()}.json"
    result.write_bytes(canonical_json_bytes(chosen.model_dump(mode="json")))
    os.chmod(result, 0o600)
    return result


def _client(serving: Path, daily_root: Path | None = None) -> TestClient:
    return TestClient(
        create_app(
            WebSettings(serving_root=serving, formula_pool_daily_result_root=daily_root),
            clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=5),
            background=False,
        )
    )


def test_list_distinguishes_unavailable_old_empty_unrun_and_success(tmp_path: Path) -> None:
    serving = tmp_path / "serving"
    with _client(serving) as client:
        unavailable = client.get(PATH, headers=HEADERS)
    assert unavailable.json()["data"]["availability"] == "unavailable"
    build_web_fixture(serving, "baseline", sequence=0)
    with _client(serving) as client:
        old = client.get(PATH, headers=HEADERS)
    assert old.json()["data"]["availability"] == "not_published"
    _publish(serving, sequence=1, state="empty")
    with _client(serving) as client:
        empty = client.get(PATH, headers=HEADERS)
    assert empty.json()["data"]["availability"] == "empty"
    assert empty.json()["data"]["pools"] == []
    _publish(serving, sequence=2, run=False)
    with _client(serving) as client:
        unrun = client.get(PATH, headers=HEADERS)
        no_members = client.get(f"{PATH}/research/members", headers=HEADERS)
    assert unrun.json()["data"]["pools"][0]["latest_result"] is None
    assert unrun.json()["data"]["pools"][0]["status_label"] == "尚未运行"
    assert no_members.status_code == 409
    _publish(serving, sequence=3)
    with _client(serving) as client:
        ready = client.get(PATH, headers=HEADERS)
    assert ready.status_code == 200
    row = ready.json()["data"]["pools"][0]
    assert row["latest_result"]["trade_date"] == DAY.isoformat()
    assert row["latest_result"]["unknown_reasons"] == [
        {"reason": "missing_date", "label": "缺少当日行情", "count": 1}
    ]
    assert "/private/" not in ready.text


def test_members_page_cursor_and_file_change_fail_closed(tmp_path: Path) -> None:
    serving, root = tmp_path / "serving", tmp_path / "daily"
    _publish(serving, sequence=0)
    result = _file(root)
    with _client(serving, root) as client:
        first = client.get(f"{PATH}/research/members", params={"page_size": 1}, headers=HEADERS)
        assert first.status_code == 200
        assert first.json()["data"]["match_codes"] == [CODES[0]]
        cursor = first.json()["data"]["next_cursor"]
        second = client.get(
            f"{PATH}/research/members",
            params={"page_size": 1, "cursor": cursor},
            headers=HEADERS,
        )
        assert second.json()["data"]["match_codes"] == [CODES[1]]
        _publish(serving, sequence=1)
        client.app.state.web.tracker.refresh()
        stale = client.get(
            f"{PATH}/research/members",
            params={"page_size": 1, "cursor": cursor},
            headers=HEADERS,
        )
        assert stale.status_code == 409
        result.write_bytes(b"{}")
        tampered = client.get(f"{PATH}/research/members", headers=HEADERS)
        assert tampered.status_code == 503
        assert "/private/" not in tampered.text


def test_members_missing_root_and_extra_query_do_not_become_empty(tmp_path: Path) -> None:
    serving = tmp_path / "serving"
    _publish(serving, sequence=0)
    with _client(serving) as client:
        missing = client.get(f"{PATH}/research/members", headers=HEADERS)
        bad_query = client.get(f"{PATH}/research/members?path=/private/secret", headers=HEADERS)
    assert missing.status_code == 503
    assert bad_query.status_code == 422


def test_trusted_zero_match_is_a_completed_result_with_empty_members(tmp_path: Path) -> None:
    serving, root = tmp_path / "serving", tmp_path / "daily"
    daily = _daily((), unknown_reasons={}, no_match_count=2)
    _publish(serving, sequence=0, daily=daily)
    _file(root, daily)
    with _client(serving, root) as client:
        listing = client.get(PATH, headers=HEADERS)
        members = client.get(f"{PATH}/research/members", headers=HEADERS)
    assert listing.status_code == members.status_code == 200
    assert listing.json()["data"]["pools"][0]["latest_result"]["match_count"] == 0
    assert listing.json()["data"]["pools"][0]["status_label"] == "已有结果"
    assert members.json()["data"]["match_codes"] == []
    assert members.json()["data"]["total"] == 0


def test_member_cursor_is_authenticated_and_the_daily_file_must_be_exact(tmp_path: Path) -> None:
    serving, root = tmp_path / "serving", tmp_path / "daily"
    _publish(serving, sequence=0)
    result = _file(root)
    with _client(serving, root) as client:
        first = client.get(f"{PATH}/research/members", params={"page_size": 1}, headers=HEADERS)
        cursor = first.json()["data"]["next_cursor"]
        invalid = ("A" if cursor[0] != "A" else "B") + cursor[1:]
        bad_cursor = client.get(
            f"{PATH}/research/members",
            params={"page_size": 1, "cursor": invalid},
            headers=HEADERS,
        )
        changed_page = client.get(
            f"{PATH}/research/members",
            params={"page_size": 2, "cursor": cursor},
            headers=HEADERS,
        )
        assert bad_cursor.status_code == changed_page.status_code == 409
        original = result.read_bytes()
        result.write_bytes(original.replace(b"000001.SZ", b"000002.SZ"))
        changed_file = client.get(f"{PATH}/research/members", headers=HEADERS)
        assert changed_file.status_code == 503
        result.unlink()
        missing_file = client.get(f"{PATH}/research/members", headers=HEADERS)
        assert missing_file.status_code == 503


def test_daily_file_over_two_mib_is_unreadable(tmp_path: Path) -> None:
    serving, root = tmp_path / "serving", tmp_path / "daily"
    _publish(serving, sequence=0)
    result = _file(root)
    result.write_bytes(b"x" * (2 * 1024 * 1024 + 1))
    with _client(serving, root) as client:
        response = client.get(f"{PATH}/research/members", headers=HEADERS)
    assert response.status_code == 503
    assert "result" not in response.text


def test_web_read_rejects_partial_manifest_and_cross_generation(tmp_path: Path) -> None:
    serving = tmp_path / "serving"
    _publish(serving, sequence=0)
    tracker = GenerationTracker(serving)
    tracker.refresh()
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            assert read_formula_pool_snapshot(borrowed)[0] == "ready"
            counts = dict(borrowed.manifest.row_counts)
            counts["formula_pool_definition"] = 0
            partial = replace(
                borrowed,
                manifest=borrowed.manifest.model_copy(update={"row_counts": counts}),
            )
            with pytest.raises(ValueError, match="status differs"):
                read_formula_pool_snapshot(partial)
            sources = dict(borrowed.manifest.source_generations)
            sources["signals"] = "0" * 64
            mixed = replace(
                borrowed,
                manifest=borrowed.manifest.model_copy(update={"source_generations": sources}),
            )
            with pytest.raises(ValueError, match="generation"):
                read_formula_pool_snapshot(mixed)
    finally:
        tracker.close()


def test_zero_row_partial_table_without_status_is_not_an_old_generation(tmp_path: Path) -> None:
    serving = tmp_path / "serving"
    build_web_fixture(serving, "baseline", sequence=0)
    tracker = GenerationTracker(serving)
    tracker.refresh()
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            manifest = borrowed.manifest.model_copy(
                update={
                    "row_counts": {
                        "formula_pool_state": 0,
                        "formula_pool_definition": 0,
                        "formula_pool_latest_result": 0,
                    }
                }
            )
            with duckdb.connect(":memory:") as connection:
                connection.execute(
                    "CREATE TABLE projection_status (table_name VARCHAR, available BOOLEAN, "
                    "row_count INTEGER, owner_dataset_id VARCHAR, owner_generation_id VARCHAR, "
                    "available_at TIMESTAMP)"
                )
                partial = replace(borrowed, manifest=manifest, cursor=connection.cursor())
                with pytest.raises(ValueError, match="physical generation"):
                    read_formula_pool_snapshot(partial)
    finally:
        tracker.close()


def test_unavailable_status_with_only_one_zero_row_physical_table_is_incomplete(
    tmp_path: Path,
) -> None:
    serving = tmp_path / "serving"
    build_web_fixture(serving, "baseline", sequence=0)
    tracker = GenerationTracker(serving)
    tracker.refresh()
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            partial = replace(
                borrowed,
                manifest=borrowed.manifest.model_copy(
                    update={"row_counts": {"formula_pool_definition": 0}}
                ),
            )
            with pytest.raises(ValueError, match="physical group"):
                read_formula_pool_snapshot(partial)
    finally:
        tracker.close()


def test_chinese_pool_name_matches_save_contract_and_members_read(tmp_path: Path) -> None:
    serving, root = tmp_path / "serving", tmp_path / "daily"
    daily = _daily(base_name="研究池")
    _publish(serving, sequence=0, daily=daily, base_name="研究池")
    _file(root, daily)
    with _client(serving, root) as client:
        response = client.get(f"{PATH}/研究池/members", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["data"]["pool_name"] == "user/研究池"
    assert response.json()["data"]["match_codes"] == list(CODES)
