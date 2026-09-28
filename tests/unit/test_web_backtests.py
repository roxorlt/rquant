"""Read-only minute replay results from one verified Serving generation."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


@pytest.fixture(scope="module")
def platform_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("backtests") / "serving"
    build_web_fixture(root, "panorama")
    return root


def _client(root: Path) -> TestClient:
    return TestClient(
        create_app(
            WebSettings(serving_root=root, stale_after_seconds=1e9),
            clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
            background=False,
        )
    )


def test_recent_runs_are_stable_bounded_and_grouped(platform_root: Path) -> None:
    with _client(platform_root) as client:
        first = client.get("/api/v1/backtests", params={"limit": 1})
        assert first.status_code == 200, first.text
        generation = first.json()["serving"]["generation_id"]
        assert first.headers["X-Rquant-Generation"] == generation
        data = first.json()["data"]
        assert data["available"] is True
        assert data["total"] == 2
        assert data["next_offset"] == 1
        assert len(data["runs"]) == 1
        assert data["runs"][0]["run_id"] == "run-later"
        assert data["runs"][0]["configurations"] == 1

        second = client.get(
            "/api/v1/backtests",
            params={"limit": 1, "offset": 1, "generation_id": generation},
        )
        assert second.status_code == 200, second.text
        assert second.json()["data"]["runs"][0]["run_id"] == "run-earlier"
        assert second.json()["data"]["runs"][0]["configurations"] == 2
        assert second.json()["data"]["next_offset"] is None
        assert client.get("/api/v1/backtests", params={"limit": 1, "offset": 1}).status_code == 422
        assert client.get("/api/v1/backtests", params={"limit": 51}).status_code == 422
        assert client.get("/api/v1/backtests", params={"offset": 2001}).status_code == 422


def test_run_detail_filters_one_configuration_and_pages_trades(platform_root: Path) -> None:
    with _client(platform_root) as client:
        generation = client.get("/api/v1/backtests").json()["serving"]["generation_id"]
        params = {"generation_id": generation, "limit": 1}
        first = client.get("/api/v1/backtests/run-earlier", params=params)
        assert first.status_code == 200, first.text
        data = first.json()["data"]
        assert data["run"]["run_id"] == "run-earlier"
        assert data["summary_available"] is True
        assert data["trades_available"] is True
        assert [row["entry_mode_label"] for row in data["groups"]] == ["突破回踩确认", "第一次突破"]
        assert data["total_trades"] == 23
        assert data["next_offset"] == 1
        assert data["trades"][0]["trade_id"] == "trade-3"
        assert data["trades"][0]["exit_reason_label"] == "止损"

        filtered = client.get(
            "/api/v1/backtests/run-earlier",
            params={**params, "entry_mode": "first_break", "profile_variant": "baseline"},
        )
        assert filtered.status_code == 200, filtered.text
        assert filtered.json()["data"]["total_trades"] == 22
        assert filtered.json()["data"]["trades"][0]["trade_id"] == "trade-29"
        assert filtered.json()["data"]["trades"][0]["exit_reason_label"] == "持有到期"
        after = client.get(
            "/api/v1/backtests/run-earlier",
            params={**params, "offset": 2},
        )
        assert after.status_code == 200, after.text
        assert [trade["trade_id"] for trade in after.json()["data"]["trades"]] == ["trade-28"]
        beyond = client.get("/api/v1/backtests/run-earlier", params={**params, "offset": 9999})
        assert beyond.status_code == 200
        assert beyond.json()["data"]["trades"] == []
        assert beyond.json()["data"]["next_offset"] is None
        assert (
            client.get(
                "/api/v1/backtests/run-earlier", params={**params, "offset": 10001}
            ).status_code
            == 422
        )
        assert (
            client.get(
                "/api/v1/backtests/run-earlier",
                params={**params, "entry_mode": "first_break"},
            ).status_code
            == 422
        )


def test_empty_missing_and_generation_change_have_distinct_results(
    platform_root: Path, tmp_path: Path
) -> None:
    with _client(platform_root) as client:
        generation = client.get("/api/v1/backtests").json()["serving"]["generation_id"]
        no_trades = client.get("/api/v1/backtests/run-later", params={"generation_id": generation})
        assert no_trades.status_code == 200, no_trades.text
        assert no_trades.json()["data"]["groups"][0]["trades"] == 0
        assert no_trades.json()["data"]["total_trades"] == 0
        assert no_trades.json()["data"]["trades"] == []
        assert (
            client.get(
                "/api/v1/backtests/missing", params={"generation_id": generation}
            ).status_code
            == 404
        )
        switched = client.get("/api/v1/backtests/run-earlier", params={"generation_id": "a" * 64})
        assert switched.status_code == 409
        assert switched.json()["detail"] == "数据已更新，请重新选择回放。"

    root = tmp_path / "baseline"
    build_web_fixture(root, "baseline")
    with _client(root) as client:
        listing = client.get("/api/v1/backtests").json()["data"]
        assert listing == {"available": False, "runs": [], "total": 0, "next_offset": None}
        generation = client.get("/api/v1/backtests").json()["serving"]["generation_id"]
        detail = client.get("/api/v1/backtests/anything", params={"generation_id": generation})
        assert detail.status_code == 200
        assert detail.json()["data"]["summary_available"] is False


def test_without_serving_is_unavailable(tmp_path: Path) -> None:
    with _client(tmp_path / "missing") as client:
        response = client.get("/api/v1/backtests")
        assert response.status_code == 200
        assert response.json()["serving"]["state"] == "unavailable"
        assert response.json()["data"]["available"] is False


def test_published_empty_run_list_and_missing_trade_projection(tmp_path: Path) -> None:
    empty_root = tmp_path / "empty"
    build_web_fixture(empty_root, "platform_empty")
    with _client(empty_root) as client:
        listing = client.get("/api/v1/backtests").json()
        assert listing["data"] == {"available": True, "runs": [], "total": 0, "next_offset": None}
        detail = client.get(
            "/api/v1/backtests/none",
            params={"generation_id": listing["serving"]["generation_id"]},
        )
        assert detail.status_code == 404

    summary_root = tmp_path / "summary-only"
    build_web_fixture(summary_root, "platform_summary_only")
    with _client(summary_root) as client:
        listing = client.get("/api/v1/backtests").json()
        detail = client.get(
            "/api/v1/backtests/run-earlier",
            params={"generation_id": listing["serving"]["generation_id"]},
        )
        assert detail.status_code == 200, detail.text
        assert detail.json()["data"]["summary_available"] is True
        assert detail.json()["data"]["trades_available"] is False
        assert len(detail.json()["data"]["groups"]) == 2
        assert detail.json()["data"]["trades"] == []


def test_page_from_previous_published_generation_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "switch"
    build_web_fixture(root, "panorama")
    app = create_app(
        WebSettings(serving_root=root, stale_after_seconds=1e9),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=2),
        background=False,
    )
    with TestClient(app) as client:
        old = client.get("/api/v1/backtests").json()["serving"]["generation_id"]
        build_web_fixture(root, "panorama", sequence=1)
        app.state.web.tracker.refresh()
        new = client.get("/api/v1/backtests").json()["serving"]["generation_id"]
        assert old != new
        stale_page = client.get("/api/v1/backtests/run-earlier", params={"generation_id": old})
        assert stale_page.status_code == 409
        stale_listing = client.get(
            "/api/v1/backtests", params={"limit": 1, "offset": 1, "generation_id": old}
        )
        assert stale_listing.status_code == 409
        current_listing = client.get(
            "/api/v1/backtests", params={"limit": 1, "offset": 1, "generation_id": new}
        )
        assert current_listing.status_code == 200
        assert current_listing.json()["data"]["runs"][0]["run_id"] == "run-earlier"
        current_page = client.get("/api/v1/backtests/run-earlier", params={"generation_id": new})
        assert current_page.status_code == 200
