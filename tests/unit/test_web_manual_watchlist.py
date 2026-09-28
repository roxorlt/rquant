from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
    build_manual_watchlist_projections,
)
from rquant.web.app import create_app
from rquant.web.manual_watchlist_read import read_manual_watchlist
from rquant.web.serving import BorrowedGeneration, GenerationTracker
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

PATH = "/api/v1/watchlist"
NOW = FIXTURE_BUILT_AT + timedelta(minutes=5)
HEADERS = {"x-rquant-user": "alice"}


def _client(root: Path, *, private: bool = True, now: datetime = NOW) -> TestClient:
    settings = WebSettings(
        serving_root=root,
        ingress_socket_path=root.parent / "private.sock" if private else None,
    )
    return TestClient(create_app(settings, clock=lambda: now, background=False))


def _publish(
    root: Path,
    *,
    sequence: int = 0,
    rows: tuple[ManualWatchlistProjectionRow, ...] = (),
    activated: bool = True,
) -> None:
    built_at = FIXTURE_BUILT_AT + timedelta(minutes=sequence)
    snapshot = (
        ManualWatchlistAuthoritySnapshot.create(
            activated_at=FIXTURE_BUILT_AT - timedelta(days=1), rows=rows
        )
        if activated
        else None
    )
    build_web_fixture(
        root,
        "baseline",
        sequence=sequence,
        signal_projections=build_manual_watchlist_projections(snapshot, observed_at=built_at),
    )


def _row(
    owner: str,
    code: str,
    version: int,
    *,
    deleted: bool = False,
    expiry: datetime | None = None,
) -> ManualWatchlistProjectionRow:
    return ManualWatchlistProjectionRow(
        owner_id=owner,
        ts_code=code,
        version=version,
        deleted=deleted,
        source=None if deleted else "detail",
        price_levels_json="[]" if deleted else '["10.00","12.345"]',
        expires_at=expiry,
        updated_at=None if deleted else FIXTURE_BUILT_AT - timedelta(minutes=1),
    )


def test_private_ingress_and_authenticated_user_are_required(tmp_path: Path) -> None:
    _publish(tmp_path / "serving")
    with _client(tmp_path / "serving", private=False) as client:
        assert client.get(PATH, headers=HEADERS).status_code == 503
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).status_code == 503
    with _client(tmp_path / "serving") as client:
        assert client.get(PATH).status_code == 401
        assert client.get(f"{PATH}/600001.SH").status_code == 401
        assert client.get(f"{PATH}/bad").status_code == 401
        assert client.get(PATH, headers={"x-rquant-user": "bad name"}).status_code == 401


def test_list_and_exact_status_are_isolated_and_keep_decimal_strings(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rows=(
            _row("alice", "600001.SH", 1),
            _row("alice", "600002.SH", 2, expiry=NOW),
            _row("alice", "600003.SH", 3, deleted=True),
            _row("bob", "600004.SH", 4),
        ),
    )
    with _client(root) as client:
        listed = client.get(PATH, headers=HEADERS)
        assert listed.status_code == 200
        data = listed.json()["data"]
        assert data["availability"] == "ready"
        assert [item["ts_code"] for item in data["items"]] == ["600001.SH"]
        assert data["items"][0]["price_levels"] == ["10.00", "12.345"]
        for code, status, version in (
            ("600001.SH", "active", 1),
            ("600002.SH", "expired", 2),
            ("600003.SH", "deleted", 3),
            ("600004.SH", "absent", None),
            ("600005.SH", "absent", None),
        ):
            exact = client.get(f"{PATH}/{code}", headers=HEADERS)
            assert exact.status_code == 200
            assert (exact.json()["data"]["status"], exact.json()["data"]["version"]) == (
                status,
                version,
            )
        bob = client.get(PATH, headers={"x-rquant-user": "bob"}).json()["data"]
        assert [item["ts_code"] for item in bob["items"]] == ["600004.SH"]


def test_request_clock_drives_expiry_and_trusted_empty_is_distinct(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 7, expiry=NOW + timedelta(seconds=1)),))
    with _client(root) as client:
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] == "active"
    with _client(root, now=NOW + timedelta(seconds=1)) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["items"] == []
        assert (
            client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] == "expired"
        )
    _publish(root, sequence=1)
    with _client(root, now=NOW + timedelta(minutes=1)) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "ready"
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] == "absent"


def test_old_unactivated_and_stale_generations_never_mean_absent(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    with _client(root) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "unavailable"
    build_web_fixture(root, "baseline")
    with _client(root) as client:
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] is None
    _publish(root, sequence=1, activated=False)
    with _client(root, now=NOW + timedelta(minutes=1)) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "unavailable"
    _publish(root, sequence=2)
    with _client(root, now=NOW + timedelta(hours=2)) as client:
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] is None


def test_owner_query_and_invalid_code_are_rejected(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    with _client(root) as client:
        assert client.get(f"{PATH}?owner_id=bob", headers=HEADERS).status_code == 422
        assert client.get(f"{PATH}/600001.SH?owner=bob", headers=HEADERS).status_code == 422
        invalid = client.get(f"{PATH}/bad", headers=HEADERS)
        assert invalid.status_code == 422
        assert invalid.json() == {"detail": "股票代码有误，请检查后重试。"}


def test_degraded_generation_is_unavailable_even_when_old_members_exist(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 1),))
    tracker = GenerationTracker(root, pointer_check_seconds=3600)
    settings = WebSettings(serving_root=root, ingress_socket_path=tmp_path / "private.sock")
    app = create_app(settings, tracker=tracker, clock=lambda: NOW, background=False)
    with TestClient(app) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "ready"
        tracker._record_failure("new generation verification failed")
        unavailable = client.get(PATH, headers=HEADERS).json()
        assert unavailable["data"]["availability"] == "unavailable"
        assert unavailable["data"]["items"] == []
        assert unavailable["serving"]["state"] == "degraded"
        assert "new generation verification failed" not in str(unavailable)


def test_manifest_count_mismatch_blocks_positive_membership(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 1),))
    tracker = GenerationTracker(root)
    tracker.refresh()
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            counts = dict(borrowed.manifest.row_counts)
            counts["manual_watchlist"] += 1
            broken = BorrowedGeneration(
                manifest=borrowed.manifest.model_copy(update={"row_counts": counts}),
                pointer=borrowed.pointer,
                cursor=borrowed.cursor,
                fallback_detail=None,
            )
            with pytest.raises(ValueError, match="manifest and status differ"):
                read_manual_watchlist(broken, owner_id="alice", now=NOW)
    finally:
        tracker.close()


def test_list_refuses_more_than_five_hundred_active_members(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    rows = tuple(_row("alice", f"{600000 + number:06d}.SH", 1) for number in range(501))
    _publish(root, rows=rows)
    with _client(root) as client:
        listed = client.get(PATH, headers=HEADERS).json()["data"]
        assert listed["availability"] == "unavailable"
        assert listed["items"] == []
        assert client.get(f"{PATH}/600000.SH", headers=HEADERS).json()["data"]["status"] == "active"
