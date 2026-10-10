"""The paper page reads complete, reconciled accounts from one Serving lease."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pytest

from rquant.paper_contracts import PaperAccountSnapshot
from rquant.serving_contracts import FreshnessStatus
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ResearcherTestClient as TestClient
from tests.support.web_proxy_identity import create_private_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

if TYPE_CHECKING:
    from fastapi import FastAPI

    from rquant.paper_portfolio_projection import PaperPortfolioPublishedAccount


def _app(root: Path):
    return create_app(
        WebSettings(serving_root=root),
        clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=30),
        background=False,
    )


def _comparison_app(
    tmp_path: Path, account: PaperPortfolioPublishedAccount, at: datetime
) -> FastAPI:
    from rquant.paper_portfolio_projection import (
        PaperPortfolioSnapshot,
        paper_portfolio_projections,
    )
    from rquant.serving_publisher import ServingPublisher
    from rquant.serving_read_models import (
        SERVING_TABLE_SPECS,
        ServingProjectionInput,
        ServingReadModelInput,
        build_serving_read_models,
    )
    from tests.support.web_serving_fixture import _generation_ids, _watermarks

    root = tmp_path / "comparison-serving"
    generations = _generation_ids("baseline", 0)
    snapshot = PaperPortfolioSnapshot(available_at=at, accounts=(account,))
    tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=at,
            paper_accounts=(account.frame.account,),
            projections=tuple(
                ServingProjectionInput.bind(
                    item,
                    owner_dataset_id="paper_accounts",
                    owner_generation_id=generations["paper_accounts"],
                )
                for item in paper_portfolio_projections(snapshot)
            ),
        )
    )
    ServingPublisher(
        root, producer_commit="0" * 40, schema_version=3, table_specs=SERVING_TABLE_SPECS
    ).publish(
        tables,
        watermarks=tuple(
            mark.model_copy(
                update={
                    "event_time": min(mark.event_time, at),
                    "published_at": min(mark.published_at, at),
                }
            )
            for mark in _watermarks("baseline", built_at=at, generations=generations, sequence=0)
        ),
        source_generations=generations,
        built_at=at,
    )
    return create_app(WebSettings(serving_root=root), clock=lambda: at, background=False)


@pytest.mark.parametrize("has_position", [False, True])
@pytest.mark.parametrize(
    "lower,upper,expected",
    [("1", "2", "inside"), ("2", "3", "outside"), ("1.795", "1.795", "inside")],
)
def test_original_paper_detail_keeps_band_position_when_optional_field_is_missing(
    tmp_path: Path, has_position: bool, lower: str, upper: str, expected: str
) -> None:
    from rquant.paper_portfolio_view_source import publish_paper_band_position
    from tests.unit.test_runtime_health_owner_metrics import _closed_comparison

    _source, original, at = _closed_comparison(tmp_path, lower=lower, upper=upper)
    account = publish_paper_band_position(original) if has_position else original
    assert (account.band_position is not None) == has_position
    app = _comparison_app(tmp_path, account, at)
    with TestClient(app, headers={"x-rquant-user": "alice"}) as client:
        response = client.get(
            f"/api/v1/paper-portfolios/{account.configuration.binding.account_id}"
        )
    assert response.status_code == 200, response.text
    assert response.json()["data"]["band_position"] == expected
    assert response.json()["data"]["band"] == account.band.model_dump(mode="json")


@pytest.mark.parametrize("incomplete", [False, True])
def test_original_paper_detail_does_not_compare_missing_nav_or_different_dates(
    tmp_path: Path, incomplete: bool
) -> None:
    from rquant.paper_portfolio_band import PaperBacktestBandResult
    from tests.unit.test_runtime_health_owner_metrics import _closed_comparison

    _source, account, at = _closed_comparison(tmp_path)
    values = account.model_dump(mode="python")
    if incomplete:
        values["nav"] = ()
    else:
        band = PaperBacktestBandResult.model_validate(
            account.band.model_dump(mode="python")
            | {"dates": (account.band.dates[0] - timedelta(days=1),)}
        )
        values["band"] = band
        summary = values["recent_research"][0]
        summary["sealed"] |= {"band": band, "result_hash": band.fingerprint}
    account = type(account).model_validate(values)
    app = _comparison_app(tmp_path, account, at)
    with TestClient(app, headers={"x-rquant-user": "alice"}) as client:
        response = client.get(
            f"/api/v1/paper-portfolios/{account.configuration.binding.account_id}"
        )
    assert response.status_code == 200, response.text
    assert response.json()["data"]["band_position"] == "unavailable"
    assert response.json()["data"]["band"] is None


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
