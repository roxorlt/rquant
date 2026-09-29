"""Experiment list reads one published generation and rejects stale page boundaries."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import (
    ProofTestClient,
    ResearcherTestClient,
    create_private_test_app,
)
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _published_window(
    *, rows: tuple[dict[str, object], ...] = ()
) -> tuple[ServingProjectionPayload, ...]:
    return (
        ServingProjectionPayload(
            table_name="experiment_attempt",
            available_at=FIXTURE_BUILT_AT,
            rows=rows,
        ),
        ServingProjectionPayload(
            table_name="experiment_attempt_window",
            available_at=FIXTURE_BUILT_AT,
            rows=(
                {
                    "snapshot_key": "current",
                    "retained_count": len(rows),
                    "truncated": False,
                    "oldest_registered_at": rows[-1]["registered_at"] if rows else None,
                },
            ),
        ),
    )


def _row(marker: str, minute: int) -> dict[str, object]:
    return {
        "experiment_id": marker * 64,
        "hypothesis_family": "均线研究",
        "registered_at": (FIXTURE_BUILT_AT - timedelta(minutes=minute)).isoformat(),
        "status": "succeeded" if marker == "a" else "registered",
        "completed_at": (FIXTURE_BUILT_AT - timedelta(minutes=minute - 1)).isoformat()
        if marker == "a"
        else None,
        "trade_count": 12 if marker == "a" else None,
        "net_return_pct": 7.5 if marker == "a" else None,
        "max_drawdown_pct": 3.25 if marker == "a" else None,
        "win_rate_pct": 60.0 if marker == "a" else None,
    }


def _app(root: Path):
    return create_private_test_app(
        WebSettings(serving_root=root, stale_after_seconds=1e9),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def test_experiment_list_requires_identity_and_pages_only_the_borrowed_generation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    build_web_fixture(
        root, "baseline", promotion_projections=_published_window(rows=(_row("a", 2), _row("b", 3)))
    )
    app = _app(root)
    with ProofTestClient(app) as anonymous:
        assert anonymous.get("/api/v1/experiments", params={"limit": 1}).status_code == 401
    with ResearcherTestClient(_app(root)) as client:
        first = client.get("/api/v1/experiments", params={"limit": 1})
        assert first.status_code == 200, first.text
        generation = first.json()["serving"]["generation_id"]
        assert first.headers["X-Rquant-Generation"] == generation
        assert first.json()["data"]["available"] is True
        assert first.json()["data"]["retained_count"] == 2
        assert first.json()["data"]["truncated"] is False
        assert first.json()["data"]["items"][0]["net_return_pct"] == 7.5
        assert "parameter_fingerprint" not in first.text
        cursor = first.json()["data"]["next_cursor"]
        assert cursor
        second = client.get(
            "/api/v1/experiments",
            params={"limit": 1, "generation_id": generation, "cursor": cursor},
        )
        assert second.status_code == 200, second.text
        assert second.json()["data"]["items"][0]["status"] == "registered"
        assert second.json()["data"]["next_cursor"] is None
        assert client.get("/api/v1/experiments", params={"cursor": cursor}).status_code == 422
        assert (
            client.get(
                "/api/v1/experiments", params={"generation_id": "f" * 64, "cursor": cursor}
            ).status_code
            == 409
        )
        assert (
            client.get(
                "/api/v1/experiments", params={"generation_id": generation, "cursor": cursor + "x"}
            ).status_code
            == 422
        )


def test_missing_source_is_not_reported_as_empty_experiment_history(tmp_path: Path) -> None:
    root = tmp_path / "missing"
    build_web_fixture(root, "baseline")
    with ResearcherTestClient(_app(root)) as client:
        data = client.get("/api/v1/experiments").json()["data"]
        assert data["available"] is False
        assert data["items"] == []

    empty = tmp_path / "empty"
    build_web_fixture(empty, "baseline", promotion_projections=_published_window())
    with ResearcherTestClient(_app(empty)) as client:
        data = client.get("/api/v1/experiments").json()["data"]
        assert data["available"] is True
        assert data["items"] == []
