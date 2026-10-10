"""Published pool returns must match the current receipt and full member set."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from rquant.serving_page_projection_source import DuckDBSignalPageProjectionSource
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
from tests.unit.test_pool_member_return import _sealed_history
from tests.unit.test_pool_result_publication import NOW, TODAY
from tests.unit.test_web_pools import (
    _membership_member,
    _membership_status,
    _receipt_response,
    _receipt_row,
    _rule_row,
)

_POOL = "user/可证池"
_CODE_A = "600001.SH"
_CODE_B = "600002.SH"


def _return_row(
    *,
    pool: str = _POOL,
    day: str = "2026-09-23",
    version: str = "r" * 64,
    code: str = _CODE_A,
    entry_day: str = "2026-09-22",
    entry_version: str = "e" * 64,
    gain: float = 12.5,
    line: float | None = 10.25,
) -> tuple[object, ...]:
    return pool, day, version, code, entry_day, entry_version, gain, line


def _response(tmp_path: Path, *, returns: list[tuple[object, ...]] | None) -> dict[str, object]:
    return _receipt_response(
        tmp_path,
        definitions=[_rule_row(_POOL)],
        hits=[(_POOL, _CODE_A), (_POOL, _CODE_B)],
        receipts=[_receipt_row(_POOL, count=2)],
        memberships=[
            _membership_status(_POOL),
            _membership_member(_POOL, _CODE_A),
            _membership_member(_POOL, _CODE_B),
        ],
        returns=returns,
    )["data"]["pools"][0]


def test_real_v2_receipt_reaches_pools_via_published_serving_and_clears_on_switch(
    tmp_path: Path,
) -> None:
    database = tmp_path / "small_ro.duckdb"
    _sealed_history(database)
    projections = DuckDBSignalPageProjectionSource(database)(NOW).projections
    root = tmp_path / "serving"
    first = build_web_fixture(root, "baseline", signal_projections=projections)
    app = create_app(
        WebSettings(serving_root=root, stale_after_seconds=600),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(minutes=2),
        background=False,
    )
    with TestClient(app) as client:
        current = client.get("/api/v1/pools")
        assert current.status_code == 200, current.text
        assert current.headers["X-Rquant-Generation"] == first.generation_id
        pool = next(
            item for item in current.json()["data"]["pools"] if item["key"] == "n-shape-pool1"
        )
        assert pool["gain_verified_count"] == 1
        assert pool["gain_sample_avg_pct"] == pytest.approx(60.0)
        member = pool["members"][0]
        assert member["entry_trade_date"] == "2026-07-31"
        assert member["entry_close"] == 10.0
        assert member["gain_pct"] == pytest.approx(60.0)
        assert member["gain_through_date"] == TODAY.isoformat()
        assert member["entry_line_price"] is None

        older_authority = tuple(
            item for item in projections if item.table_name != "pool_member_return"
        )
        second = build_web_fixture(root, "baseline", sequence=1, signal_projections=older_authority)
        app.state.web.tracker.refresh()
        switched = client.get("/api/v1/pools")
        assert switched.status_code == 200, switched.text
        assert second.generation_id != first.generation_id
        assert switched.headers["X-Rquant-Generation"] == second.generation_id
        member = next(
            item for item in switched.json()["data"]["pools"] if item["key"] == "n-shape-pool1"
        )["members"][0]
        assert member["entry_trade_date"] == "2026-07-31"
        assert member["entry_close"] == 10.0
        assert member["gain_pct"] is None
        assert member["entry_line_price"] is None


def test_optional_return_and_partial_coverage_preserve_verified_entry(tmp_path: Path) -> None:
    old = _response(tmp_path / "old", returns=None)
    assert old["gain_verified_count"] == 0
    assert old["gain_sample_avg_pct"] is None
    assert all(member["entry_close"] == 10.25 for member in old["members"])
    assert all(member["gain_pct"] is None for member in old["members"])

    partial = _response(tmp_path / "partial", returns=[_return_row()])
    assert partial["member_count"] == 2
    assert partial["gain_verified_count"] == 1
    assert partial["gain_sample_avg_pct"] == 12.5
    members = {member["code"]: member for member in partial["members"]}
    assert members[_CODE_A]["gain_pct"] == 12.5
    assert members[_CODE_A]["gain_through_date"] == "2026-09-23"
    assert members[_CODE_A]["entry_line_price"] == 10.25
    assert members[_CODE_B]["entry_trade_date"] == "2026-09-22"
    assert members[_CODE_B]["entry_close"] == 10.25
    assert members[_CODE_B]["gain_pct"] is None


@pytest.mark.parametrize(
    "returns",
    [
        [_return_row(), _return_row()],
        [_return_row(version="x" * 64)],
        [_return_row(entry_version="x" * 64)],
        [_return_row(day="2026-09-22")],
        [_return_row(entry_day="2026-09-21")],
        [_return_row(code="600099.SH")],
        [_return_row(line=12.0)],
        [_return_row(gain=float("nan"))],
    ],
    ids=[
        "duplicate",
        "current-version",
        "entry-version",
        "current-day",
        "entry-day",
        "outside-member-set",
        "wrong-line",
        "nonfinite-gain",
    ],
)
def test_invalid_return_identity_hides_gain_but_not_entry(
    tmp_path: Path, returns: list[tuple[object, ...]]
) -> None:
    pool = _response(tmp_path, returns=returns)
    assert pool["gain_verified_count"] == 0
    assert pool["gain_sample_avg_pct"] is None
    assert all(member["entry_close"] == 10.25 for member in pool["members"])
    assert all(member["gain_pct"] is None for member in pool["members"])
