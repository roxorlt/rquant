"""The factor result API reads one verified Serving generation only."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from rquant.factor.definition import build_factor_definition
from rquant.factor.definition_serving import project_factor_definition_serving_snapshot
from rquant.factor.registry import FactorDefinitionRegistry, SaveFactorDefinitionRequest
from rquant.factor.result_serving import project_factor_result_projections
from rquant.factor.serving_projection import project_factor_definition_projections
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.routes.factor_results import _read_results
from rquant.web.serving import GenerationTracker
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
from tests.unit.test_factor_job_ledger import _research_and_spec
from tests.unit.test_factor_result_serving import _success


def _app(root: Path):  # type: ignore[no-untyped-def]
    return create_app(
        WebSettings(serving_root=root, stale_after_seconds=1e9),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def _definition_pair(tmp_path: Path, *, changed: bool) -> tuple[ServingProjectionPayload, ...]:
    spec = _research_and_spec()[1]
    original = spec.adapter_request.definition
    definition = (
        original
        if not changed
        else build_factor_definition(
            factor_id=original.factor_id,
            name_zh=original.name_zh,
            category=original.category,
            direction=original.direction,
            version=original.version,
            earliest_available_date=original.earliest_available_date,
            expression="close * 2",
            feature_catalog=original.feature_catalog,
        )
    )
    registry = FactorDefinitionRegistry(tmp_path / "registry.sqlite3")
    registry.initialize()
    registry.save(
        SaveFactorDefinitionRequest(
            command_id="save-price-factor", definition=definition, expected_head=None
        ),
        expected_identity=registry.identity(),
    )
    snapshot = project_factor_definition_serving_snapshot(
        registry, expected_identity=registry.identity(), available_at=FIXTURE_BUILT_AT
    )
    return project_factor_definition_projections(snapshot)


def test_results_success_unpublished_empty_and_generation_switch(tmp_path: Path) -> None:
    serving = tmp_path / "serving"
    build_web_fixture(serving, "baseline")
    with ResearcherTestClient(_app(serving)) as client:
        response = client.get("/api/v1/factors/results")
        assert response.status_code == 200, response.text
        assert response.json()["data"] == {
            "availability": "unavailable",
            "available_at": None,
            "results": [],
        }

    factor_root = tmp_path / "factor"
    factor_root.mkdir(mode=0o700)
    identity, artifacts, job_id, _completion = _success(factor_root)
    group = project_factor_result_projections(identity, artifacts, available_at=FIXTURE_BUILT_AT)
    build_web_fixture(serving, "baseline", sequence=1, factor_result_projections=group)
    with ResearcherTestClient(_app(serving)) as client:
        response = client.get("/api/v1/factors/results")
        assert response.status_code == 200, response.text
        generation = response.json()["serving"]["generation_id"]
        assert response.headers["X-Rquant-Generation"] == generation
        data = response.json()["data"]
        assert data["availability"] == "populated"
        assert len(data["results"]) == 1
        assert data["results"][0]["job_id"] == job_id
        assert data["results"][0]["display_status"] == "available"
        detail = client.get(
            f"/api/v1/factors/results/{job_id}", params={"generation_id": generation}
        )
        assert detail.status_code == 200, detail.text
        assert detail.json()["data"]["availability"] == "ready"
        assert detail.json()["data"]["research"]["ic_points"]
        assert detail.json()["data"]["research"]["coverage_days"]
        assert detail.json()["data"]["research"]["pool_label"] == "固定样本"
        assert "历史回溯研究" in detail.json()["data"]["research"]["basis_label"]
        for private in ("factor-jobs.sqlite3", str(artifacts), identity.instance_id):
            assert private not in detail.text
        assert (
            client.get("/api/v1/factors/results", params={"generation_id": "a" * 64}).status_code
            == 409
        )

        build_web_fixture(serving, "baseline", sequence=2)
        client.app.state.web.tracker.refresh()
        assert (
            client.get(
                f"/api/v1/factors/results/{job_id}", params={"generation_id": generation}
            ).status_code
            == 409
        )
        now = client.get("/api/v1/factors/results")
        assert now.status_code == 200
        assert now.json()["data"]["availability"] == "unavailable"


def test_exact_definition_digest_is_required_for_current_name(tmp_path: Path) -> None:
    factor = tmp_path / "factor"
    factor.mkdir(mode=0o700)
    identity, artifacts, _job_id, _completion = _success(factor)
    group = project_factor_result_projections(identity, artifacts, available_at=FIXTURE_BUILT_AT)
    root = tmp_path / "serving"
    same = tmp_path / "same"
    same.mkdir()
    build_web_fixture(
        root,
        "baseline",
        factor_result_projections=group,
        factor_definition_projections=_definition_pair(same, changed=False),
    )
    with ResearcherTestClient(_app(root)) as client:
        row = client.get("/api/v1/factors/results").json()["data"]["results"][0]
        assert row["definition_status"] == "current"
        assert row["factor_name_zh"] == "价格因子"

    changed = tmp_path / "changed"
    changed.mkdir()
    build_web_fixture(
        root,
        "baseline",
        sequence=1,
        factor_result_projections=group,
        factor_definition_projections=_definition_pair(changed, changed=True),
    )
    with ResearcherTestClient(_app(root)) as client:
        row = client.get("/api/v1/factors/results").json()["data"]["results"][0]
        assert row["factor_id"] == "price_factor"
        assert row["factor_version"] == 1
        assert row["definition_status"] == "historical_unavailable"
        assert row["factor_name_zh"] is None


def test_web_rejects_bad_factor_result_state_before_returning_charts(tmp_path: Path) -> None:
    factor = tmp_path / "factor"
    factor.mkdir(mode=0o700)
    identity, artifacts, _job_id, _completion = _success(factor)
    root = tmp_path / "serving"
    build_web_fixture(
        root,
        "baseline",
        factor_result_projections=project_factor_result_projections(
            identity, artifacts, available_at=FIXTURE_BUILT_AT
        ),
    )
    tracker = GenerationTracker(root)
    tracker.refresh()
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None

            def execute(sql: str, params: Any = None) -> Any:
                rows = borrowed.cursor.execute(sql, params).fetchall()
                if "FROM factor_result_state" in sql:
                    changed = [list(row) for row in rows]
                    changed[0][-1] = "0" * 64
                    rows = [tuple(row) for row in changed]
                return SimpleNamespace(fetchall=lambda: rows)

            with pytest.raises(HTTPException) as error:
                _read_results(replace(borrowed, cursor=SimpleNamespace(execute=execute)))
            assert error.value.status_code == 503
    finally:
        tracker.close()
