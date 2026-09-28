"""The paper page reads complete, reconciled accounts from one Serving lease."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from rquant.paper_contracts import PaperAccountSnapshot
from rquant.serving_contracts import FreshnessStatus
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture


def _app(root: Path):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def test_paper_account_snapshot_reconciles_holdings_in_one_generation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")

    assert response.status_code == 200, response.text
    body = response.json()
    assert response.headers["X-Rquant-Generation"] == body["serving"]["generation_id"]
    assert body["data"]["source_state"] == "ready"
    assert len(body["data"]["accounts"]) == 1
    account = body["data"]["accounts"][0]
    assert account["account_id"] == "shadow-main"
    assert account["nav"] == 100042.0
    assert account["cash"] == 97620.0
    assert account["market_value"] == 2422.0
    assert account["unrealized_pnl"] == 42.0
    assert [holding["code"] for holding in account["holdings"]] == [
        "600005.SH",
        "600001.SH",
    ]
    assert account["holdings"][0]["name"] == "样本05"
    assert account["holdings"][0]["available_quantity"] == 100
    assert account["holdings"][1]["available_quantity"] == 0
    assert "snapshot_id" not in response.text


def test_missing_generation_is_not_a_zero_balance(tmp_path: Path) -> None:
    with TestClient(_app(tmp_path / "missing")) as client:
        response = client.get("/api/v1/paper/accounts")
    assert response.status_code == 200
    assert response.json()["data"] == {
        "source_state": "unavailable",
        "source_updated_at": None,
        "source_note": None,
        "valuation_note": None,
        "accounts": [],
        "history": {
            "source_state": "unavailable",
            "source_updated_at": None,
            "source_note": None,
            "account_id": None,
            "total_orders": None,
            "has_more": False,
            "newest_updated_at": None,
            "oldest_updated_at": None,
            "orders": [],
        },
    }


def test_multiple_accounts_and_zero_holdings_are_separate_and_switch_with_generation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    cash_only = PaperAccountSnapshot(
        account_id="cash-only",
        as_of_time=FIXTURE_BUILT_AT - timedelta(minutes=1),
        cash=Decimal("5000.00"),
        available_cash=Decimal("5000.00"),
        frozen_cash=Decimal("0"),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        nav=Decimal("5000.00"),
    )
    from tests.support import web_serving_fixture as fixture

    build_web_fixture(
        root,
        "baseline",
        paper_accounts=(
            fixture._paper_account(FIXTURE_BUILT_AT - timedelta(seconds=30)),
            cash_only,
        ),
    )
    app = create_app(
        WebSettings(serving_root=root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=90),
        background=False,
    )
    with TestClient(app) as client:
        first = client.get("/api/v1/paper/accounts")
        assert first.status_code == 200, first.text
        first_data = first.json()["data"]
        assert [row["account_id"] for row in first_data["accounts"]] == [
            "shadow-main",
            "cash-only",
        ]
        assert first_data["accounts"][1]["nav"] == 5000.0
        assert first_data["accounts"][1]["holdings"] == []

        build_web_fixture(root, "baseline", sequence=1, paper_accounts=())
        app.state.web.tracker.refresh()
        second = client.get("/api/v1/paper/accounts")
        assert second.status_code == 200
        assert second.json()["data"]["source_state"] == "empty"
        assert second.json()["data"]["accounts"] == []
        assert second.headers["X-Rquant-Generation"] != first.headers["X-Rquant-Generation"]


def test_unavailable_watermark_is_unpublished_and_stale_watermark_is_explained(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support import web_serving_fixture as fixture

    original = fixture._watermarks

    def marked(*args: object, **kwargs: object):
        marks = original(*args, **kwargs)
        return tuple(
            mark.model_copy(
                update={
                    "status": FreshnessStatus.STALE,
                    "reason": "last execution price / internal-source-name",
                }
            )
            if mark.dataset_id == "paper_accounts"
            else mark
            for mark in marks
        )

    monkeypatch.setattr(fixture, "_watermarks", marked)
    stale = tmp_path / "stale"
    build_web_fixture(stale, "baseline")
    with TestClient(_app(stale)) as client:
        response = client.get("/api/v1/paper/accounts")
    assert response.status_code == 200
    assert response.json()["data"]["source_state"] == "ready"
    assert "更新延迟" in response.json()["data"]["source_note"]
    assert "最近成交价" in response.json()["data"]["valuation_note"]
    assert "internal-source-name" not in response.text

    def unpublished(*args: object, **kwargs: object):
        marks = original(*args, **kwargs)
        return tuple(
            mark.model_copy(
                update={"status": FreshnessStatus.UNAVAILABLE, "reason": "source unavailable"}
            )
            if mark.dataset_id == "paper_accounts"
            else mark
            for mark in marks
        )

    monkeypatch.setattr(fixture, "_watermarks", unpublished)
    root = tmp_path / "unpublished"
    build_web_fixture(root, "baseline")
    with TestClient(_app(root)) as client:
        response = client.get("/api/v1/paper/accounts")
    assert response.status_code == 200
    assert response.json()["data"]["source_state"] == "not_published"
    assert response.json()["data"]["accounts"] == []


@pytest.mark.parametrize("fault", ["missing", "orphan", "amount"])
def test_unreadable_or_inconsistent_published_tables_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    app = _app(root)

    class FaultCursor:
        def __init__(self, cursor: object):
            self.cursor = cursor
            self.rows: list[tuple[object, ...]] = []

        def execute(self, sql: str, params: tuple[object, ...] = ()):
            if fault == "missing" and "FROM paper_holdings" in sql:
                raise duckdb.CatalogException("secret-source-table")
            self.rows = self.cursor.execute(sql, params).fetchall()
            if "FROM paper_holdings" in sql and self.rows:
                first = self.rows[0]
                if fault == "orphan":
                    self.rows[0] = ("other-account", *first[1:])
                elif fault == "amount":
                    self.rows[0] = (*first[:7], first[7] + Decimal("1"), *first[8:])
            return self

        def fetchall(self):
            return self.rows

    with TestClient(app) as client:
        original_borrow = app.state.web.tracker.borrow

        @contextmanager
        def broken_borrow():
            with original_borrow() as borrowed:
                assert borrowed is not None
                yield replace(borrowed, cursor=FaultCursor(borrowed.cursor))

        monkeypatch.setattr(app.state.web.tracker, "borrow", broken_borrow)
        response = client.get("/api/v1/paper/accounts")
    assert response.status_code == 503
    assert response.json()["detail"] == "模拟账户数据暂时无法读取，请稍后重试。"
    assert "secret-source-table" not in response.text
