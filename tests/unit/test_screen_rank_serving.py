"""Ranked daily results must reach Serving only from sealed persisted facts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

from rquant.pool_definition_projection import build_pool_definition_rows
from rquant.pool_result_receipt import (
    ScreenRunReceipt,
    member_price_digest,
    member_rank_digest,
    member_set_digest,
)
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    _read_verified_run_receipts,
)
from rquant.serving_read_models import ServingProjectionPayload
from tests.unit.test_serving_page_projection_source import NOW, _signal_projection_database


def _ranked_replica(path: Path) -> ScreenRunReceipt:
    _signal_projection_database(path)
    prices = [("600000.SH", 10.6), ("600001.SH", 12.0), ("600002.SH", 15.0)]
    ranks = [
        ("600000.SH", 3, 88.5),
        ("600001.SH", 2, 90.0),
        ("600002.SH", 1, 95.0),
    ]
    definition_version = next(
        str(row["version"])
        for row in build_pool_definition_rows({}, {}, root_path="")
        if row["pool_name"] == "n-shape-pool1"
    )
    receipt = ScreenRunReceipt(
        contract="screen-run-receipt/v3",
        trade_date=date(2026, 7, 31),
        preset_name="n-shape-pool1",
        definition_version=definition_version,
        hit_count=3,
        member_digest=member_set_digest([code for code, _ in prices]),
        price_digest=member_price_digest(prices),
        rank_digest=member_rank_digest(ranks),
        lineage_complete=True,
        completed_at=datetime(2026, 7, 31, 7, 10, tzinfo=UTC),
    )
    prior = ScreenRunReceipt(
        contract="screen-run-receipt/v2",
        trade_date=date(2026, 7, 30),
        preset_name="n-shape-pool1",
        definition_version=definition_version,
        hit_count=0,
        member_digest=member_set_digest([]),
        price_digest=member_price_digest([]),
        lineage_complete=True,
        completed_at=datetime(2026, 7, 30, 7, 10, tzinfo=UTC),
    )
    with duckdb.connect(str(path)) as connection:
        connection.execute("ALTER TABLE screen_result ADD COLUMN rank_position INTEGER")
        connection.execute("ALTER TABLE screen_result ADD COLUMN ranking_score DOUBLE")
        connection.execute(
            "UPDATE screen_result SET rank_position = 3, ranking_score = 88.5 "
            "WHERE trade_date = '2026-07-31'"
        )
        connection.execute(
            "INSERT INTO screen_result VALUES "
            "('2026-07-31', 'n-shape-pool1', '600001.SH', '样本一', 12.0, 2.0, '{}', "
            "'2026-07-31 15:05:00', 2, 90.0), "
            "('2026-07-31', 'n-shape-pool1', '600002.SH', '样本二', 15.0, 3.0, '{}', "
            "'2026-07-31 15:05:00', 1, 95.0)"
        )
        connection.execute(
            "CREATE TABLE trade_calendar (exchange VARCHAR, cal_date DATE, "
            "is_open BOOLEAN, updated_at TIMESTAMPTZ)"
        )
        connection.execute(
            "INSERT INTO trade_calendar VALUES "
            "('SSE', '2026-07-30', true, '2026-07-29T12:00:00Z'), "
            "('SSE', '2026-07-31', true, '2026-07-30T12:00:00Z')"
        )
        connection.execute(
            "CREATE TABLE daily_bar (trade_date DATE, ts_code VARCHAR, close DOUBLE, vol DOUBLE)"
        )
        connection.execute(
            "INSERT INTO daily_bar VALUES "
            "('2026-07-31', '600000.SH', 10.6, 1000.0), "
            "('2026-07-31', '600001.SH', 12.0, 1000.0), "
            "('2026-07-31', '600002.SH', 15.0, 1000.0)"
        )
        connection.execute(
            "CREATE TABLE adj_factor (trade_date DATE, ts_code VARCHAR, adj_factor DOUBLE)"
        )
        connection.execute(
            "INSERT INTO adj_factor VALUES "
            "('2026-07-31', '600000.SH', 1.0), "
            "('2026-07-31', '600001.SH', 1.0), "
            "('2026-07-31', '600002.SH', 1.0)"
        )
        connection.execute(
            "CREATE TABLE screen_run_receipt (trade_date DATE, preset_name VARCHAR, "
            "definition_version VARCHAR, parent_trade_date DATE, parent_result_version VARCHAR, "
            "hit_count INTEGER, member_digest VARCHAR, lineage_complete BOOLEAN, "
            "completed_at TIMESTAMPTZ, result_version VARCHAR, contract VARCHAR, "
            "price_digest VARCHAR, rank_digest VARCHAR)"
        )
        connection.executemany(
            "INSERT INTO screen_run_receipt VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    item.trade_date,
                    item.preset_name,
                    item.definition_version,
                    item.parent_trade_date,
                    item.parent_result_version,
                    item.hit_count,
                    item.member_digest,
                    item.lineage_complete,
                    item.completed_at,
                    item.result_version,
                    item.contract,
                    item.price_digest,
                    item.rank_digest,
                )
                for item in (prior, receipt)
            ],
        )
    return receipt


def _published(path: Path) -> dict[str, ServingProjectionPayload]:
    return {
        item.table_name: item for item in DuckDBSignalPageProjectionSource(path)(NOW).projections
    }


def _pool_hit(projections: dict[str, ServingProjectionPayload]) -> Mapping[str, object]:
    return next(
        row
        for row in projections["canvas_hit"].rows
        if row["preset_name"] == "n-shape-pool1"
    )


def test_valid_v3_publishes_persisted_rank_and_price_from_one_replica(tmp_path: Path) -> None:
    path = tmp_path / "replica.duckdb"
    receipt = _ranked_replica(path)

    projections = _published(path)
    ranks = {
        hit["ts_code"]: json.loads(hit["row_json"])
        for hit in projections["canvas_hit"].rows
        if hit["preset_name"] == "n-shape-pool1"
    }
    assert {
        code: (row["rank_position"], row["ranking_score"])
        for code, row in ranks.items()
    } == {
        "600000.SH": (3, 88.5),
        "600001.SH": (2, 90.0),
        "600002.SH": (1, 95.0),
    }
    assert all(row["rank_result_version"] == receipt.result_version for row in ranks.values())
    with duckdb.connect(str(path), read_only=True) as connection:
        verified = _read_verified_run_receipts(
            connection,
            cutoff=datetime(2026, 8, 3, 16, 0),
            observed=NOW,
            generation_sealed_before_cutoff=True,
        )
    assert verified is not None
    assert receipt.result_version in verified.price_digest_verified
    membership = [
        row for row in projections["pool_membership"].rows
        if row["pool_name"] == "n-shape-pool1" and row["row_kind"] == "member"
    ]
    assert {row["ts_code"]: row["entry_close"] for row in membership} == {
        "600000.SH": 10.6,
        "600001.SH": 12.0,
        "600002.SH": 15.0,
    }
    returns = [
        row for row in projections["pool_member_return"].rows
        if row["pool_name"] == "n-shape-pool1"
    ]
    assert {row["ts_code"]: row["gain_pct"] for row in returns} == {
        "600000.SH": 0.0,
        "600001.SH": 0.0,
        "600002.SH": 0.0,
    }


@pytest.mark.parametrize(
    "mutation",
    (
        "UPDATE screen_result SET ranking_score = 89.0 WHERE ts_code = '600001.SH'",
        "UPDATE screen_result SET close = 12.1 WHERE ts_code = '600001.SH'",
        "UPDATE screen_result SET rank_position = 2 WHERE ts_code = '600000.SH'",
        "DELETE FROM screen_result WHERE ts_code = '600001.SH'",
        "UPDATE screen_run_receipt SET rank_digest = repeat('0', 64) "
        "WHERE trade_date = '2026-07-31'",
        "ALTER TABLE screen_run_receipt DROP COLUMN rank_digest",
        "ALTER TABLE screen_result DROP COLUMN rank_position",
    ),
)
def test_bad_v3_proof_never_publishes_partial_or_prior_rank(
    tmp_path: Path, mutation: str
) -> None:
    path = tmp_path / "replica.duckdb"
    _ranked_replica(path)
    with duckdb.connect(str(path)) as connection:
        connection.execute(mutation)

    projections = _published(path)
    rows = [
        json.loads(hit["row_json"])
        for hit in projections["canvas_hit"].rows
        if hit["preset_name"] == "n-shape-pool1"
    ]
    assert len(rows) == (2 if mutation.startswith("DELETE") else 3)
    assert all("rank_position" not in row and "ranking_score" not in row for row in rows)
    assert not projections["screen_run_receipt"].rows


def test_v2_replica_without_rank_columns_keeps_price_and_members(tmp_path: Path) -> None:
    path = tmp_path / "replica.duckdb"
    current = _ranked_replica(path)
    v2 = ScreenRunReceipt(
        contract="screen-run-receipt/v2",
        trade_date=current.trade_date,
        preset_name=current.preset_name,
        definition_version=current.definition_version,
        hit_count=current.hit_count,
        member_digest=current.member_digest,
        price_digest=current.price_digest,
        lineage_complete=True,
        completed_at=current.completed_at,
    )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "UPDATE screen_run_receipt SET contract = ?, result_version = ?, rank_digest = NULL "
            "WHERE trade_date = '2026-07-31'",
            (v2.contract, v2.result_version),
        )
        connection.execute("ALTER TABLE screen_run_receipt DROP COLUMN rank_digest")
        connection.execute("ALTER TABLE screen_result DROP COLUMN rank_position")
        connection.execute("ALTER TABLE screen_result DROP COLUMN ranking_score")

    projections = _published(path)
    hit = _pool_hit(projections)
    assert "rank_position" not in json.loads(hit["row_json"])
    receipt = projections["screen_run_receipt"].rows[0]
    assert receipt["result_version"] == v2.result_version
    member = next(
        row for row in projections["pool_membership"].rows
        if row["pool_name"] == "n-shape-pool1" and row["row_kind"] == "member"
    )
    assert member["entry_close"] == 10.6
