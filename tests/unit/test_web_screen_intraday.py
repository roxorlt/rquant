from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from rquant.web.models.screen import ScreenCondition, ScreenRunRequest
from rquant.web.screen_service import ScreenApplicationError, ScreenApplicationService
from rquant.web.serving import BorrowedGeneration
from tests.unit.test_serving_screen_intraday import _source_world


def _borrow(snapshot):
    from rquant.screen.intraday_source import intraday_projections

    cursor = duckdb.connect(":memory:")
    cursor.execute(
        "CREATE TABLE projection_status(table_name VARCHAR,available BOOLEAN,"
        "row_count INTEGER,available_at TIMESTAMPTZ)"
    )
    for projection in intraday_projections(snapshot):
        if projection.table_name == "market_snapshot":
            cursor.execute(
                "CREATE TABLE market_snapshot(as_of TIMESTAMPTZ,ts_code VARCHAR,name VARCHAR,"
                "price DOUBLE,open DOUBLE,high DOUBLE,low DOUBLE,pre_close DOUBLE,pct_chg DOUBLE,"
                "volume DOUBLE,amount DOUBLE)"
            )
        elif projection.table_name == "intraday_screen_source":
            cursor.execute(
                "CREATE TABLE intraday_screen_source(source_identity VARCHAR,trade_date DATE,"
                "cutoff TIMESTAMPTZ,payload_json VARCHAR)"
            )
        else:
            cursor.execute(
                "CREATE TABLE intraday_feature_snapshot(source_identity VARCHAR,"
                "ts_code VARCHAR,payload_json VARCHAR)"
            )
        cursor.execute(
            "INSERT INTO projection_status VALUES(?,true,?,?)",
            [projection.table_name, len(projection.rows), projection.available_at],
        )
        for row in projection.rows:
            cursor.execute(
                f"INSERT INTO {projection.table_name} VALUES({','.join('?' for _ in row)})",
                list(row.values()),
            )
    return BorrowedGeneration(
        manifest=SimpleNamespace(generation_id="9" * 64, built_at=snapshot.source.cutoff),
        pointer=None,
        cursor=cursor,
        fallback_detail=None,
    )


def test_intraday_catalog_and_complete_run_use_observed_prices_and_unknown_coverage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, cutoff = _source_world(tmp_path, monkeypatch)
    borrowed = _borrow(reader(cutoff))
    try:
        service = ScreenApplicationService(cursor_key=b"k" * 32, clock=lambda: cutoff)
        catalog = service.catalog(borrowed, mode="intraday")
        assert catalog.available and catalog.source.mode == "intraday"
        assert catalog.source.daily_anchor_date.isoformat() == "2026-07-30"
        assert catalog.source.identity != borrowed.manifest.generation_id
        body = ScreenRunRequest(
            mode="intraday",
            trade_date=catalog.dates[0],
            source_identity=catalog.source.identity,
            decision_cutoff=cutoff,
            intraday_source_identity=catalog.source.intraday_source_identity,
            conditions=[ScreenCondition(key="gt", args={"left": "INTRADAY_PRICE[0]", "right": 10})],
        )
        result = service.run(body, borrowed=borrowed, serving_unavailable=False)
        assert result.base_count == 2 and result.total == 1 and result.unknown_count == 1
        assert result.rows[0].close == 14
        assert result.source == catalog.source
        from rquant.llm.schemas import RuleCall
        from rquant.screen.query_contracts import ScreenQueryDefinition

        complete = service.run_complete(
            ScreenQueryDefinition(
                mode="intraday",
                trade_date=body.trade_date,
                source_kind="intraday",
                source_identity=body.source_identity,
                cutoff=cutoff,
                conditions=(RuleCall(name="gt", args=body.conditions[0].args),),
            ),
            borrowed=borrowed,
            serving_unavailable=False,
        )
        assert complete.rows == result.rows and complete.unknown_count == 1
    finally:
        borrowed.cursor.close()


def test_intraday_request_cannot_change_cutoff_or_use_daily_source_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, cutoff = _source_world(tmp_path, monkeypatch)
    borrowed = _borrow(reader(cutoff))
    try:
        service = ScreenApplicationService(cursor_key=b"k" * 32, clock=lambda: cutoff)
        catalog = service.catalog(borrowed, mode="intraday")
        body = ScreenRunRequest(
            mode="intraday",
            trade_date=catalog.dates[0],
            source_identity=catalog.source.identity,
            decision_cutoff=cutoff + timedelta(seconds=1),
            intraday_source_identity=catalog.source.intraday_source_identity,
            conditions=[ScreenCondition(key="gt", args={"left": "INTRADAY_PRICE[0]", "right": 10})],
        )
        with pytest.raises(ScreenApplicationError) as error:
            service.run(body, borrowed=borrowed, serving_unavailable=False)
        assert error.value.status_code == 409
        body = body.model_copy(
            update={"decision_cutoff": cutoff, "source_identity": borrowed.manifest.generation_id}
        )
        with pytest.raises(ScreenApplicationError) as error:
            service.run(body, borrowed=borrowed, serving_unavailable=False)
        assert error.value.status_code == 409
    finally:
        borrowed.cursor.close()


def test_intraday_ranking_counts_only_actual_known_members(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.web.models.screen import ScreenRankingCondition, ScreenRankingPlan

    reader, cutoff = _source_world(tmp_path, monkeypatch)
    borrowed = _borrow(reader(cutoff))
    try:
        service = ScreenApplicationService(cursor_key=b"k" * 32, clock=lambda: cutoff)
        catalog = service.catalog(borrowed, mode="intraday")
        body = ScreenRunRequest(
            mode="intraday",
            trade_date=catalog.dates[0],
            source_identity=catalog.source.identity,
            decision_cutoff=cutoff,
            intraday_source_identity=catalog.source.intraday_source_identity,
            conditions=[ScreenCondition(key="gt", args={"left": 11, "right": 10})],
            ranking=ScreenRankingPlan(
                top_n=2,
                conditions=[
                    ScreenRankingCondition(metric="INTRADAY_PRICE[0]", ascending=True, weight=100)
                ],
            ),
        )
        result = service.run(body, borrowed=borrowed, serving_unavailable=False)
        assert result.total == 2 and result.ranked_count == 2 and len(result.rows) == 2
        assert result.rows[1].rank_position == 2
        assert result.unknown_count == 1
    finally:
        borrowed.cursor.close()


def test_missing_intraday_table_returns_unavailable_without_daily_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    reader, cutoff = _source_world(tmp_path, monkeypatch)
    borrowed = _borrow(reader(cutoff))
    try:
        service = ScreenApplicationService(cursor_key=b"k" * 32, clock=lambda: cutoff)
        borrowed.cursor.execute("DROP TABLE intraday_feature_snapshot")
        catalog = service.catalog(borrowed, mode="intraday")
        assert (
            not catalog.available and catalog.source is None and catalog.source_kind == "intraday"
        )
    finally:
        borrowed.cursor.close()
