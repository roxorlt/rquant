"""Formula task reads use one Serving generation and an authenticated result file."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.formula_market_job_projection import (
    FormulaMarketArtifactIndexRow,
    FormulaMarketJobRow,
    FormulaMarketJobSnapshot,
    FormulaMarketJobStateRow,
    project_formula_market_job,
)
from rquant.screen.formula_market_jobs import FormulaMarketJobResult
from rquant.screen.formula_market_run import FormulaMarketRunSummary
from rquant.strict_json import canonical_json_bytes
from rquant.web.app import create_app
from rquant.web.serving import BorrowedGeneration
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

PATH = "/api/v1/screen/tdx/market/jobs"
HEADERS = {"x-rquant-user": "researcher"}
TASK_ID = "a" * 32
REQUEST_SHA = "b" * 64
UNIVERSE = "c" * 64
PROJECTION = "d" * 64
FORMULA = "CLOSE>2"
TRADE_DATE = date(2026, 9, 24)
AVAILABLE_AT = FIXTURE_BUILT_AT - timedelta(minutes=1)


def _snapshot(status: str, root: Path) -> FormulaMarketJobSnapshot:
    created = FIXTURE_BUILT_AT - timedelta(minutes=5)
    formula_sha = hashlib.sha256(FORMULA.encode()).hexdigest()
    result_sha = None
    artifacts: tuple[FormulaMarketArtifactIndexRow, ...] = ()
    if status == "succeeded":
        summary = FormulaMarketRunSummary(
            trade_date=TRADE_DATE,
            decision_at=created,
            universe_identity=UNIVERSE,
            universe_completed_at=created - timedelta(minutes=1),
            projection_identity=PROJECTION,
            projection_updated_at=created - timedelta(minutes=1),
            market_total=3,
            listed_count=2,
            paused_count=1,
            match_count=2,
            no_match_count=0,
            unknown_count=1,
            unknown_reasons={"missing_projection_code": 1},
            match_codes=("000001.SZ", "830001.BJ"),
        )
        result = FormulaMarketJobResult.create(
            task_id=TASK_ID,
            request_sha256=REQUEST_SHA,
            formula_sha256=formula_sha,
            summary=summary,
        )
        payload = canonical_json_bytes(result.model_dump(mode="json"))
        result_sha = result.content_sha256
        root.mkdir(mode=0o700, exist_ok=True)
        filename = f"formula-market-v1-{TASK_ID}-{result_sha}.json"
        file_path = root / filename
        file_path.write_bytes(payload)
        os.chmod(file_path, 0o600)
        artifacts = (
            FormulaMarketArtifactIndexRow(
                rank=0,
                task_id=TASK_ID,
                relative_path=filename,
                content_sha256=result_sha,
                byte_count=len(payload),
                request_sha256=REQUEST_SHA,
                formula_sha256=formula_sha,
                universe_identity=UNIVERSE,
                projection_identity=PROJECTION,
                trade_date=TRADE_DATE,
                decision_at=created,
                market_total=3,
                listed_count=2,
                paused_count=1,
                match_count=2,
                no_match_count=0,
                unknown_count=1,
            ),
        )
    jobs = (
        FormulaMarketJobRow(
            rank=0,
            task_id=TASK_ID,
            status=status,
            attempts=0 if status == "queued" else 1,
            created_at=created,
            updated_at=created + timedelta(minutes=1),
            result_sha256=result_sha,
            error_code="internal_error" if status == "failed" else None,
            request_sha256=REQUEST_SHA,
            formula_sha256=formula_sha,
            formula=FORMULA,
            trade_date=TRADE_DATE,
            decision_at=created,
            universe_identity=UNIVERSE,
            projection_identity=PROJECTION,
        ),
    )
    return FormulaMarketJobSnapshot(
        state=FormulaMarketJobStateRow(
            availability="ready",
            total_task_count=1,
            retained_task_count=1,
            has_older_tasks=False,
        ),
        jobs=jobs,
        artifacts=artifacts,
        available_at=AVAILABLE_AT,
    )


def _client(serving: Path, result_root: Path | None) -> TestClient:
    return TestClient(
        create_app(
            WebSettings(serving_root=serving, formula_market_result_root=result_root),
            clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=5),
            background=False,
        )
    )


def _publish(serving: Path, status: str | None, result_root: Path, sequence: int) -> None:
    projections = (
        () if status is None else project_formula_market_job(_snapshot(status, result_root))
    )
    build_web_fixture(
        serving,
        "baseline",
        sequence=sequence,
        formula_market_projections=projections,
    )


def test_old_empty_and_unavailable_serving_have_distinct_task_states(tmp_path: Path) -> None:
    serving = tmp_path / "serving"
    with _client(serving, None) as client:
        unavailable = client.get(PATH, headers=HEADERS)
    assert unavailable.status_code == 200
    assert unavailable.json()["data"]["availability"] == "unavailable"

    _publish(serving, None, tmp_path / "results", sequence=0)
    with _client(serving, None) as client:
        old = client.get(PATH, headers=HEADERS)
    assert old.status_code == 200
    assert old.json()["data"]["availability"] == "not_published"

    empty = FormulaMarketJobSnapshot(
        state=FormulaMarketJobStateRow(
            availability="empty",
            total_task_count=0,
            retained_task_count=0,
            has_older_tasks=False,
        ),
        available_at=AVAILABLE_AT,
    )
    build_web_fixture(
        serving,
        "baseline",
        sequence=1,
        formula_market_projections=project_formula_market_job(empty),
    )
    with _client(serving, None) as client:
        response = client.get(PATH, headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["data"]["availability"] == "empty"
    assert response.json()["data"]["jobs"] == []

    source_missing = FormulaMarketJobSnapshot(
        state=FormulaMarketJobStateRow(
            availability="unavailable",
            total_task_count=0,
            retained_task_count=0,
            has_older_tasks=False,
        ),
        available_at=AVAILABLE_AT,
    )
    build_web_fixture(
        serving,
        "baseline",
        sequence=2,
        formula_market_projections=project_formula_market_job(source_missing),
    )
    with _client(serving, None) as client:
        missing = client.get(PATH, headers=HEADERS)
    assert missing.status_code == 200
    assert missing.json()["data"]["availability"] == "unavailable"


def test_queued_then_successful_result_is_bounded_and_paginated(tmp_path: Path) -> None:
    serving, results = tmp_path / "serving", tmp_path / "results"
    _publish(serving, "queued", results, sequence=0)
    with _client(serving, results) as client:
        queued = client.get(f"{PATH}/{TASK_ID}", headers=HEADERS)
    assert queued.status_code == 200
    assert queued.json()["data"]["job"]["status"] == "queued"
    assert queued.json()["data"]["summary"] is None

    _publish(serving, "succeeded", results, sequence=1)
    with _client(serving, results) as client:
        listing = client.get(PATH, headers=HEADERS)
        detail = client.get(f"{PATH}/{TASK_ID}", headers=HEADERS)
        first = client.get(f"{PATH}/{TASK_ID}/matches", params={"page_size": 1}, headers=HEADERS)
        cursor = first.json()["data"]["next_cursor"]
        second = client.get(
            f"{PATH}/{TASK_ID}/matches",
            params={"page_size": 1, "cursor": cursor},
            headers=HEADERS,
        )
    assert (
        listing.status_code == detail.status_code == first.status_code == second.status_code == 200
    )
    assert len(listing.json()["data"]["jobs"]) == 1
    summary = detail.json()["data"]["summary"]
    assert (summary["market_total"], summary["match_count"], summary["unknown_count"]) == (3, 2, 1)
    assert summary["unknown_reasons"] == [
        {"reason": "missing_projection_code", "label": "缺少行情资料", "count": 1}
    ]
    assert first.json()["data"]["match_codes"] == ["000001.SZ"]
    assert second.json()["data"]["match_codes"] == ["830001.BJ"]
    assert second.json()["data"]["next_cursor"] is None
    assert "unknown_codes" not in detail.text


def test_failed_task_has_actionable_hint_without_result(tmp_path: Path) -> None:
    serving, results = tmp_path / "serving", tmp_path / "results"
    _publish(serving, "failed", results, sequence=0)
    with _client(serving, None) as client:
        detail = client.get(f"{PATH}/{TASK_ID}", headers=HEADERS)
        matches = client.get(f"{PATH}/{TASK_ID}/matches", headers=HEADERS)
    assert detail.status_code == 200
    assert detail.json()["data"]["summary"] is None
    assert detail.json()["data"]["job"]["status_label"] == "未完成"
    assert "重试" in detail.json()["data"]["job"]["hint"]
    assert matches.status_code == 409


def test_cursor_rejects_new_generation_and_oversized_page(tmp_path: Path) -> None:
    serving, results = tmp_path / "serving", tmp_path / "results"
    _publish(serving, "succeeded", results, sequence=0)
    with _client(serving, results) as client:
        first = client.get(f"{PATH}/{TASK_ID}/matches", params={"page_size": 1}, headers=HEADERS)
        cursor = first.json()["data"]["next_cursor"]
        invalid_size = client.get(
            f"{PATH}/{TASK_ID}/matches", params={"page_size": 101}, headers=HEADERS
        )
        _publish(serving, "succeeded", results, sequence=1)
        client.app.state.web.tracker.refresh()
        changed = client.get(
            f"{PATH}/{TASK_ID}/matches",
            params={"page_size": 1, "cursor": cursor},
            headers=HEADERS,
        )
    assert invalid_size.status_code == 422
    assert changed.status_code == 409
    assert "重新" in changed.json()["detail"]


def test_success_requires_configured_untampered_result(tmp_path: Path) -> None:
    serving, results = tmp_path / "serving", tmp_path / "results"
    _publish(serving, "succeeded", results, sequence=0)
    with _client(serving, None) as client:
        listing = client.get(PATH, headers=HEADERS)
        unconfigured = client.get(f"{PATH}/{TASK_ID}", headers=HEADERS)
    assert listing.status_code == 200
    assert listing.json()["data"]["jobs"][0]["status_label"] == "结果暂不可用"
    assert listing.json()["data"]["jobs"][0]["result_available"] is False
    assert unconfigured.status_code == 503
    with _client(serving, results) as client:
        valid = client.get(f"{PATH}/{TASK_ID}", headers=HEADERS)
        artifact = next(results.iterdir())
        artifact.write_bytes(artifact.read_bytes().replace(b"000001.SZ", b"000002.SZ"))
        os.chmod(artifact, 0o600)
        damaged = client.get(f"{PATH}/{TASK_ID}", headers=HEADERS)
        no_matches = client.get(f"{PATH}/{TASK_ID}/matches", headers=HEADERS)
    assert valid.status_code == 200
    assert damaged.status_code == no_matches.status_code == 503
    assert "match_codes" not in damaged.text + no_matches.text


def test_same_generation_index_identity_mismatch_is_not_served(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serving, results = tmp_path / "serving", tmp_path / "results"
    _publish(serving, "succeeded", results, sequence=0)

    class _TamperCursor:
        def __init__(self, real: object) -> None:
            self.real = real
            self.index_query = False

        def execute(self, sql: str, parameters: object = ()) -> _TamperCursor:
            self.index_query = "FROM research_artifact_index" in sql
            self.real.execute(sql, parameters)
            return self

        def fetchall(self) -> list[tuple[object, ...]]:
            rows = self.real.fetchall()
            if self.index_query:
                row = list(rows[0])
                row[7] = "e" * 64  # request digest differs from the task in this same generation
                return [tuple(row)]
            return rows

    with _client(serving, results) as client:
        tracker = client.app.state.web.tracker
        original = tracker.borrow

        @contextmanager
        def tampered() -> Iterator[BorrowedGeneration]:
            with original() as borrowed:
                assert borrowed is not None
                yield replace(borrowed, cursor=_TamperCursor(borrowed.cursor))

        monkeypatch.setattr(tracker, "borrow", tampered)
        detail = client.get(f"{PATH}/{TASK_ID}", headers=HEADERS)
        listing = client.get(PATH, headers=HEADERS)
    assert detail.status_code == listing.status_code == 503
    assert "match_codes" not in detail.text + listing.text


def test_manifest_row_count_mismatch_is_not_served(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serving, results = tmp_path / "serving", tmp_path / "results"
    _publish(serving, "succeeded", results, sequence=0)
    with _client(serving, results) as client:
        tracker = client.app.state.web.tracker
        original = tracker.borrow

        @contextmanager
        def bad_manifest() -> Iterator[BorrowedGeneration]:
            with original() as borrowed:
                assert borrowed is not None
                counts = {**borrowed.manifest.row_counts, "research_artifact_index": 0}
                yield replace(
                    borrowed,
                    manifest=borrowed.manifest.model_copy(update={"row_counts": counts}),
                )

        monkeypatch.setattr(tracker, "borrow", bad_manifest)
        response = client.get(PATH, headers=HEADERS)
    assert response.status_code == 503


def test_browser_cannot_supply_result_root(tmp_path: Path) -> None:
    serving, results = tmp_path / "serving", tmp_path / "results"
    _publish(serving, "succeeded", results, sequence=0)
    with _client(serving, results) as client:
        response = client.get(PATH, params={"result_root": "/private/other"}, headers=HEADERS)
    assert response.status_code == 422
