"""The factor catalog reads one verified Serving generation, never the registry."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

from rquant.factor.definition import build_factor_definition
from rquant.factor.definition_serving import project_factor_definition_serving_snapshot
from rquant.factor.expression import FeatureCatalog
from rquant.factor.registry import (
    ArchiveFactorRequest,
    FactorDefinitionRegistry,
    FactorHeadRef,
    SaveFactorDefinitionRequest,
)
from rquant.factor.serving_projection import project_factor_definition_projections
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.serving import GenerationTracker
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient, ResearcherTestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _pair(tmp_path: Path, *, populated: bool) -> tuple[ServingProjectionPayload, ...]:
    registry = FactorDefinitionRegistry(tmp_path / "factor-fixture.sqlite3")
    registry.initialize()
    if populated:
        for factor_id, name, expression in (
            ("a_factor", "价量动量", "ts_mean(close, 5) / ref(volume, 2)"),
            ("z_factor", "成交变化", "ts_mean(volume, 3)"),
        ):
            registry.save(
                SaveFactorDefinitionRequest(
                    command_id=f"save-{factor_id}",
                    definition=build_factor_definition(
                        factor_id=factor_id,
                        name_zh=name,
                        category="technical",
                        direction="higher_is_better",
                        version=1,
                        earliest_available_date=date(2024, 1, 2),
                        expression=expression,
                        feature_catalog=FeatureCatalog(columns=("close", "volume")),
                    ),
                    expected_head=None,
                ),
                expected_identity=registry.identity(),
            )
        head = registry.get_head("z_factor", expected_identity=registry.identity())
        assert head is not None
        registry.archive(
            ArchiveFactorRequest(
                command_id="archive-z-factor",
                factor_id="z_factor",
                expected_head=FactorHeadRef(
                    version=head.head.version,
                    content_sha256=head.head.content_sha256,
                ),
            ),
            expected_identity=registry.identity(),
        )
    snapshot = project_factor_definition_serving_snapshot(
        registry, expected_identity=registry.identity(), available_at=FIXTURE_BUILT_AT
    )
    return project_factor_definition_projections(snapshot)


def _app(root: Path) -> Any:
    return create_app(
        WebSettings(serving_root=root, stale_after_seconds=1e9),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def test_private_catalog_populated_empty_unpublished_and_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root, "baseline", factor_definition_projections=_pair(tmp_path, populated=True)
    )
    with ProofTestClient(_app(root)) as anonymous:
        assert anonymous.get("/api/v1/factors/definitions").status_code == 401
    app = _app(root)
    with ResearcherTestClient(app) as client:
        response = client.get("/api/v1/factors/definitions")
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        assert data["availability"] == "populated"
        assert [(row["name_zh"], row["archived"]) for row in data["definitions"]] == [
            ("价量动量", False),
            ("成交变化", True),
        ]
        assert data["definitions"][0]["expression"] == "ts_mean(close, 5) / ref(volume, 2)"
        assert len(data["definitions"][0]["content_sha256"]) == 64
        assert data["definitions"][0]["dependency_columns"] == ["close", "volume"]
        assert "registry_instance_id" not in response.text
        assert "factor-fixture.sqlite3" not in response.text
        generation = response.json()["serving"]["generation_id"]
        assert response.headers["X-Rquant-Generation"] == generation
        assert (
            client.get(
                "/api/v1/factors/definitions", params={"generation_id": "a" * 64}
            ).status_code
            == 409
        )

        (tmp_path / "next").mkdir()
        build_web_fixture(
            root,
            "baseline",
            sequence=1,
            factor_definition_projections=_pair(tmp_path / "next", populated=False),
        )
        app.state.web.tracker.refresh()
        empty = client.get("/api/v1/factors/definitions")
        assert empty.status_code == 200, empty.text
        assert empty.json()["data"]["availability"] == "empty"
        assert empty.json()["data"]["definitions"] == []
        assert (
            client.get(
                "/api/v1/factors/definitions", params={"generation_id": generation}
            ).status_code
            == 409
        )

        build_web_fixture(root, "baseline", sequence=2)
        app.state.web.tracker.refresh()
        unpublished = client.get("/api/v1/factors/definitions")
        assert unpublished.status_code == 200, unpublished.text
        assert unpublished.json()["data"] == {
            "availability": "unavailable",
            "definitions": [],
            "available_at": None,
        }


@pytest.mark.parametrize("fault", ["missing", "digest", "count", "owner", "time"])
def test_catalog_rejects_broken_serving_pair(tmp_path: Path, fault: str) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root, "baseline", factor_definition_projections=_pair(tmp_path, populated=True)
    )
    tracker = GenerationTracker(root)
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            cursor = borrowed.cursor

            def execute(sql: str, params: tuple[object, ...] = ()) -> Any:
                rows = cursor.execute(sql, params).fetchall()
                if "FROM projection_status" in sql:
                    changed = [list(row) for row in rows]
                    if fault == "missing":
                        changed.pop()
                    elif fault == "count":
                        changed[0][2] = 3
                    elif fault == "owner":
                        changed[0][3] = "signals"
                    elif fault == "time":
                        changed[0][5] = changed[0][5] - timedelta(seconds=1)
                    rows = [tuple(row) for row in changed]
                elif fault == "digest" and "FROM factor_definition_state" in sql:
                    changed = [list(row) for row in rows]
                    changed[0][-1] = "0" * 64
                    rows = [tuple(row) for row in changed]
                return SimpleNamespace(fetchall=lambda: rows)

            from rquant.web.routes.factors import _read_catalog

            with pytest.raises(HTTPException) as error:
                _read_catalog(replace(borrowed, cursor=SimpleNamespace(execute=execute)))
            assert error.value.status_code == 503
    finally:
        tracker.close()
