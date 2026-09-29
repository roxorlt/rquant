"""Private strategy catalog reads only a complete, same-generation projection pair."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient, ResearcherTestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _projections() -> tuple[ServingProjectionPayload, ...]:
    return (
        ServingProjectionPayload(
            table_name="strategy_catalog",
            available_at=FIXTURE_BUILT_AT,
            rows=(
                {
                    "strategy_id": "auction_gap",
                    "name": "集合竞价跳空",
                    "version": 1,
                    "registered_at": FIXTURE_BUILT_AT.isoformat(),
                },
                {
                    "strategy_id": "growth_board_surge",
                    "name": "科创及创业板放量",
                    "version": 1,
                    "registered_at": FIXTURE_BUILT_AT.isoformat(),
                },
                {
                    "strategy_id": "n_shape",
                    "name": "N 字形态",
                    "version": 1,
                    "registered_at": FIXTURE_BUILT_AT.isoformat(),
                },
            ),
        ),
        ServingProjectionPayload(
            table_name="strategy_catalog_parameter",
            available_at=FIXTURE_BUILT_AT,
            rows=(
                {
                    "strategy_id": "auction_gap",
                    "parameter_key": "min_gap_pct",
                    "label": "跳空幅度下限",
                    "display_value": "0%",
                },
                {
                    "strategy_id": "growth_board_surge",
                    "parameter_key": "allowed_boards",
                    "label": "适用板块",
                    "display_value": "创业板、科创板",
                },
                {
                    "strategy_id": "n_shape",
                    "parameter_key": "expires_seconds",
                    "label": "信号有效期",
                    "display_value": "120 秒",
                },
            ),
        ),
    )


def _app(root: Path):
    return create_app(
        WebSettings(serving_root=root, stale_after_seconds=1e9),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def test_private_catalog_reads_complete_pair_and_rejects_old_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", strategy_catalog_projections=_projections())
    with ProofTestClient(_app(root)) as anonymous:
        assert anonymous.get("/api/v1/strategies").status_code == 401
    with ResearcherTestClient(_app(root)) as client:
        first = client.get("/api/v1/strategies")
        assert first.status_code == 200, first.text
        data = first.json()["data"]
        assert data["available"] is True
        assert [item["name"] for item in data["strategies"]] == [
            "集合竞价跳空",
            "科创及创业板放量",
            "N 字形态",
        ]
        assert data["strategies"][2]["parameters"][0]["display_value"] == "120 秒"
        assert "definition_registry_root" not in first.text
        assert "registration_fingerprint" not in first.text
        generation = first.json()["serving"]["generation_id"]
        assert first.headers["X-Rquant-Generation"] == generation
        assert (
            client.get("/api/v1/strategies", params={"generation_id": "a" * 64}).status_code == 409
        )


def test_missing_catalog_is_unavailable_then_new_generation_can_remove_old_detail(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline", strategy_catalog_projections=_projections())
    app = _app(root)
    with ResearcherTestClient(app) as client:
        old = client.get("/api/v1/strategies").json()["serving"]["generation_id"]
        build_web_fixture(root, "baseline", sequence=1)
        app.state.web.tracker.refresh()
        changed = client.get("/api/v1/strategies")
        assert changed.status_code == 200, changed.text
        assert changed.json()["data"] == {"available": False, "strategies": []}
        assert changed.json()["serving"]["generation_id"] != old
        assert client.get("/api/v1/strategies", params={"generation_id": old}).status_code == 409
